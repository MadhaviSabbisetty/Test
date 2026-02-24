
commonutilities service

ContextRbacFilter.java

package com.fincore.commonutilities.security;

import com.fincore.commonutilities.jwt.JwtUtil;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.lang.NonNull;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;
import java.util.Set;


/**
 * A Blocking (Servlet) Filter that enforces security for microservices.
 * * Responsibilities:
 * 1. Validate JWT Signature.
 * 2. Enforce "One Session Per User" by comparing JWT 'jti' with Redis 'USR:<userId>'.
 * 3. Enforce RBAC (Role Based Access Control) via Redis permissions.
 */
@Slf4j
@RequiredArgsConstructor
public class ContextRbacFilter extends OncePerRequestFilter {

    private static final String REDIS_USER_PREFIX = "USR:";
    private static final String REDIS_RBAC_PREFIX = "RBAC::PERMISSIONS::";

    private static final Set<String> WHITELIST = Set.of(
            "/api/auth/**", "/actuator/**", "/error", "/swagger-ui/**", "/v3/api-docs/**", "/api/login",
            "/api/auth/login", "/api/auth/logout", "/api/auth/check-user", "/swagger-ui.html"
    );

    private final StringRedisTemplate redisTemplate;
    private final JwtUtil jwtUtil;
    private final AntPathMatcher pathMatcher = new AntPathMatcher();

    @Override
    protected void doFilterInternal(
            @NonNull HttpServletRequest request,
            @NonNull HttpServletResponse response,
            @NonNull FilterChain filterChain
    ) throws ServletException, IOException {

        String uri = request.getRequestURI();

        // 1. SKIP WHITELIST
        for (String pattern : WHITELIST) {
            if (pathMatcher.match(pattern, uri)) {
                filterChain.doFilter(request, response);
                return;
            }
        }

        // 2. VALIDATE TOKEN
        String token = request.getHeader("Authorization");

        if (token != null) {
            try {
                // A. STANDARD VALIDATION (Signature + Expiry)
                if (!jwtUtil.isTokenValid(token)) {
                    throw new JwtException("Invalid Token Signature");
                }

                // If valid, extract info
                String userId = jwtUtil.getUserIdFromToken(token);
                String incomingJti = jwtUtil.extractClaim(removeBearer(token), Claims::getId);
                int roleIdInt = jwtUtil.getUserRoleFromToken(token);

                // B. ACTIVE SESSION CHECK
                if (!checkRedisSession(response, userId, incomingJti)) {
                    return; // Error sent
                }

                // C. RBAC / CONTEXT SETUP
                setSecurityContext(userId, roleIdInt);

                if (!checkRbac(request, response, String.valueOf(roleIdInt), userId)) {
                    return;
                }

            } catch (ExpiredJwtException e) {
                // -----------------------------------------------------------------
                // HANDLE EXPIRED TOKEN: Distinguish between "Time out" vs "Concurrent Login"
                // -----------------------------------------------------------------
                try {
                    Claims claims = e.getClaims();
                    String userId = claims.getSubject();
                    String incomingJti = claims.getId();

                    // CHECK 1: Missing JTI ---
                    if (incomingJti == null) {
                        log.error("Expired Token missing JTI for user {}", userId);
                        sendError(response, HttpServletResponse.SC_UNAUTHORIZED, "INVALID_TOKEN", "Token missing JTI");
                        return;
                    }

                    String activeJti = redisTemplate.opsForValue().get(REDIS_USER_PREFIX + userId);

                    // CASE 1: Session totally gone from Redis (User idle > 24h)
                    if (activeJti == null) {
                        log.warn("Session expired in Redis (found during token expiry check) for user {}", userId);
                        sendError(response, HttpServletResponse.SC_UNAUTHORIZED, "SESSION_EXPIRED", "Session timed out.");
                        return;
                    }

                    // CASE 2: Concurrent Login (Redis has newer JTI)
                    if (!activeJti.equals(incomingJti)) {
                        log.warn("Concurrent Login Detected on Expired Token. User: {}", userId);
                        sendError(response, HttpServletResponse.SC_UNAUTHORIZED, "CONCURRENT_LOGIN", "Logged in on another device.");
                        return;
                    }

                    // CASE 3: Normal Expiry (Redis matches) -> Allow Frontend to Refresh
                } catch (Exception ex) {
                    log.error("Error checking concurrency on expired token", ex);
                }

                // Default behavior: Tell frontend token is expired so it can call /refresh-token
                sendError(response, HttpServletResponse.SC_UNAUTHORIZED, "TOKEN_EXPIRED", "JWT has expired.");
                return;

            } catch (Exception e) {
                log.error("Auth Error: {}", e.getMessage());
                sendError(response, HttpServletResponse.SC_UNAUTHORIZED, "AUTH_ERROR", "Authentication failed.");
                return;
            }
        } else {
            sendError(response, HttpServletResponse.SC_UNAUTHORIZED, "MISSING_TOKEN", "Authorization header missing.");
            return;
        }

        filterChain.doFilter(request, response);
    }

    private boolean checkRedisSession(HttpServletResponse response, String userId, String incomingJti) throws IOException {
        if (incomingJti == null) {
            sendError(response, HttpServletResponse.SC_UNAUTHORIZED, "INVALID_TOKEN", "Token missing JTI");
            return false;
        }
        try {
            String activeJti = redisTemplate.opsForValue().get(REDIS_USER_PREFIX + userId);
            if (activeJti == null) {
                sendError(response, HttpServletResponse.SC_UNAUTHORIZED, "SESSION_EXPIRED", "Session timed out.");
                return false;
            }
            if (!incomingJti.equals(activeJti)) {
                sendError(response, HttpServletResponse.SC_UNAUTHORIZED, "CONCURRENT_LOGIN", "Logged in on another device.");
                return false;
            }
        } catch (RedisConnectionFailureException e) {
            sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "SYSTEM_ERROR", "Auth Service Unavailable");
            return false;
        }
        return true;
    }

    private void setSecurityContext(String userId, int roleId) {
        if (SecurityContextHolder.getContext().getAuthentication() == null) {
            UsernamePasswordAuthenticationToken authToken = new UsernamePasswordAuthenticationToken(
                    userId, null, List.of(new SimpleGrantedAuthority("ROLE_" + roleId))
            );
            SecurityContextHolder.getContext().setAuthentication(authToken);
        }
    }

    /**
     * Checks if the user's role has permission to access the requested Method + URL + Context.
     */
    private boolean checkRbac(HttpServletRequest request, HttpServletResponse response, String roleId, String userId) throws IOException {

        String uri = request.getRequestURI();
        String method = request.getMethod().toUpperCase();

        // DETERMINE CONTEXT (Header)
        String contextHeader = request.getHeader("X-Request-Type");

        // If header is missing, treat as "*" (Global Context)
        String requestContext = (contextHeader != null && !contextHeader.trim().isEmpty()) ? contextHeader : "*";

        // REDIS AUTHORIZATION CHECK
        String redisKey = REDIS_RBAC_PREFIX + roleId;

        // Fetch permissions set from Redis
        // Format: "METHOD:URL_PATTERN|CONTEXT" -> e.g., "POST:/user/create|USER_MANAGEMENT"
        Set<String> permissions = redisTemplate.opsForSet().members(redisKey);

        boolean isAuthorized = false;

        if (permissions != null) {
            for (String perm : permissions) {
                // Format: "METHOD:URL_PATTERN|CONTEXT"
                String[] parts = perm.split("\\|");
                if (parts.length < 2) continue;

                String methodAndUrl = parts[0];
                String allowedContext = parts[1];

                // --- A. CONTEXT CHECK ---
                // 1. Exact Match: Header "SEGMENT_CODE" matches Permission "SEGMENT_CODE"
                // 2. Wildcard Match: Permission has "*" (e.g. Reports), allows any header (or no header)
                if (!allowedContext.equals(requestContext) && !allowedContext.equals("*")) {
                    continue;
                }

                // --- B. URL & METHOD CHECK ---
                String[] muParts = methodAndUrl.split(":", 2);
                if (muParts.length < 2) continue;

                String allowedMethod = muParts[0];
                String allowedUrlPattern = muParts[1];

                // Method Match AND URL Pattern Match
                if (allowedMethod.equals(method) && pathMatcher.match(allowedUrlPattern, uri)) {
                    isAuthorized = true;
                    break;
                }

            }
        }

        if (!isAuthorized) {
            log.warn("RBAC Deny \uD83D\uDEAB: User {} (Role {}) -> {} {} (Context: {})", userId, roleId, method, uri, requestContext);
            sendError(response, HttpServletResponse.SC_FORBIDDEN, "ACCESS_DENIED", "Insufficient Permissions for this resource.");
            return false;
        }
        return true;
    }

    private void sendError(HttpServletResponse response, int status, String error, String message) throws IOException {
        response.setStatus(status);
        response.setContentType("application/json");
        response.getWriter().write(String.format("{\"status\": %d, \"error\": \"%s\", \"message\": \"%s\"}", status, error, message));
    }

    private String removeBearer(String token) {
        if (token != null && token.startsWith("Bearer ")) {
            return token.substring(7);
        }
        return token;
    }
}


jwtutil.java
package com.fincore.commonutilities.jwt;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.util.Date;
import java.util.function.Function;

/**
 * Utility class for manipulating and validating JSON Web Tokens (JWT).
 * This class is shared across microservices to ensure consistent token parsing logic.
 */
@Component
@Slf4j
public class JwtUtil {

    // Default value provided for dev, override in application.properties
    @Value("${jwt.secret:bWV0aGlvbnlsdGhyZW9ueWx0aHJlb255bGdsdXRhbWlueWxhbGFueWw=}")
    private String secretKey;


    /**
     * Extracts the User ID (Subject) from the token.
     *
     * @param token The Bearer token.
     * @return The userId string.
     */
    public String getUserIdFromToken(String token) {
        String cleanToken = removeBearerPrefix(token);
        return extractClaim(cleanToken, Claims::getSubject);
    }

    /**
     * Extracts the Role ID from the token claims.
     *
     * @param token The Bearer token.
     * @return The Role ID as an integer.
     */
    public int getUserRoleFromToken(String token) {
        String cleanToken = removeBearerPrefix(token);
        return extractClaim(cleanToken, claims -> {
            Object role = claims.get("role");
            if (role instanceof Number) {
                return ((Number) role).intValue();
            }
            // Fallback for string roles if necessary
            if (role instanceof String) {
                try {
                    return Integer.parseInt((String) role);
                } catch (NumberFormatException ignored) {
                }
            }
            throw new IllegalArgumentException("Token role claim is not a number");
        });
    }

    /**
     * Validates the cryptographic signature and expiration of the token.
     *
     * @param token The Bearer token.
     * @return true if valid, false if expired or tampered.
     */
    public boolean isTokenValid(String token) {
        String cleanToken = removeBearerPrefix(token);
        return !isTokenExpired(cleanToken);
    }

    /**
     * GENERIC CLAIM EXTRACTOR (Made Public for Filter access).
     * This is required to extract 'jti' (JWT ID) for session validation.
     */
    public <T> T extractClaim(String token, Function<Claims, T> claimsResolver) {
        final Claims claims = extractAllClaims(token);
        return claimsResolver.apply(claims);
    }

    // --- Internal Helpers ---

    private String removeBearerPrefix(String token) {
        if (token != null && token.startsWith("Bearer ")) {
            return token.substring(7);
        }
        return token;
    }

    private boolean isTokenExpired(String token) {
        return extractExpiration(token).before(new Date());
    }

    private Date extractExpiration(String token) {
        return extractClaim(token, Claims::getExpiration);
    }

    private Claims extractAllClaims(String token) {
        return Jwts.parser()
                .verifyWith(getSigningKey())
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    private SecretKey getSigningKey() {
        byte[] keyBytes = Decoders.BASE64.decode(secretKey);
        return Keys.hmacShaKeyFor(keyBytes);
    }
}

