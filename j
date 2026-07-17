package com.fincore.ReportService.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fincore.ReportService.dto.*;
import com.fincore.commonutilities.jwt.JwtUtil;
import lombok.extern.slf4j.Slf4j;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fincore.ReportService.service.ReportService;
@Slf4j
@RestController
@RequestMapping("/api/reports")
public class ReportController {

        private final ReportService reportService;
        private final JwtUtil jwtUtil; // Declare an instance field

        public ReportController(ReportService reportService, JwtUtil jwtUtil) {
                this.reportService = reportService;
                this.jwtUtil = jwtUtil;
        }
        @PostMapping("/types")
        public List<ReportTypeDto> getReportTypes(@RequestHeader("Authorization") String token,
                        @RequestBody ReportTypesRequest request) {
                log.info("Received request to fetch all available report types for" +
                                "module: {} for role: {}", request.getModuleType(),
                                jwtUtil.getUserRoleFromToken(token));
                return reportService.getReportTypes(jwtUtil.getUserRoleFromToken(token), request.getModuleType());
        }
        @PostMapping("/download")
        public ResponseEntity<StreamingResponseBody> downloadReport(
                        @RequestHeader("Authorization") String token,
                        @RequestBody ReportDownloadRequest request) throws IOException {

                log.info("Received request to download report from HDFS for this criteria: {}", request);

                ReportStreamResponse response = reportService.downloadReportStream(
                                request.getFileName(),
                                request.getDate(),
                                jwtUtil.getUserRoleFromToken(token),
                                jwtUtil.getUserIdFromToken(token),
                                request.getBranchCode());

                return ResponseEntity.ok()
                                // This header tells the browser "This is a file download"
                                .header(HttpHeaders.CONTENT_DISPOSITION,
                                                "attachment; filename=\"" + response.getDownloadFileName() + "\"")
                                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                                .body(response.getStreamBody());
        }
        @PostMapping("/create-reports")
        public Map<String, String> createReports(@RequestHeader("Authorization") String token,
                        @RequestBody ReportCreationDto requestBody) throws IOException {
                // EXTRACT USER ID FROM TOKEN
                String userId = jwtUtil.getUserIdFromToken(token);
                return reportService.createReports(requestBody, userId);
        }
        @PostMapping("/generate-reports")
        public ReportGenerationResponseDTO generateReports(@RequestBody ReportGenerationDTO requestBody)
                        throws IOException {

                return reportService.generateReports(requestBody);
        }
        @PostMapping("/download-temp-reports")
        public ResponseEntity<StreamingResponseBody> downloadTemporaryReports(
                        @RequestHeader("Authorization") String token,
                        @RequestBody ReportDownloadRequest request) throws IOException {

                log.info("Received request to download temp report from HDFS for this criteria: {}", request);

                ReportStreamResponse response = reportService.downloadTempReportStream(
                                request.getFileName(),
                                request.getDate(),
                                jwtUtil.getUserRoleFromToken(token),
                                jwtUtil.getUserIdFromToken(token),
                                request.getRunId());

                return ResponseEntity.ok()
                                // This header tells the browser "This is a file download"
                                .header(HttpHeaders.CONTENT_DISPOSITION,
                                                "attachment; filename=\"" + response.getDownloadFileName() + "\"")
                                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                                .body(response.getStreamBody());
        }
        @PostMapping("/generate/base64")
        public ResponseEntity<ReportBase64ResponseDTO> generateReportBase64(
                        @RequestBody ReportBase64RequestDTO request) throws IOException {

                ReportBase64ResponseDTO response = reportService.generateReportBase64(request);

                return ResponseEntity.ok(response);
        }
}
