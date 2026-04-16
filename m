controller file
package com.tcs.fincore.AsciiGenerationService.Controller;

import com.tcs.fincore.AsciiGenerationService.DTO.ReportRequest;
import com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/ascii")
public class AsciiController {

    @Autowired
    private AsciiGenerationService service;

    @PostMapping("/generate")
    public ResponseEntity<String> generate(@RequestBody ReportRequest request) {
        try {
            if (request.getId() == null) {
                return ResponseEntity.badRequest().body("Error: 'id' is missing");
            }

            String status = service.initiateBatch(request.getId());

            return ResponseEntity.accepted().body("Success: " + status);

        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Error: " + e.getMessage());
        }
    }
}


dto file 1 file job
package com.tcs.fincore.AsciiGenerationService.DTO;

import java.nio.file.Path;

public class FileJob {
    private Long configId;
    private Path filePath;
    private String batchId; // <--- This was missing

    // The constructor must accept 3 arguments now
    public FileJob(Long configId, Path filePath, String batchId) {
        this.configId = configId;
        this.filePath = filePath;
        this.batchId = batchId;
    }

    public Long getConfigId() { return configId; }
    public Path getFilePath() { return filePath; }
    public String getBatchId() { return batchId; } // <--- This getter was missing
}

dto file 2 report request
package com.tcs.fincore.AsciiGenerationService.DTO;

import lombok.Data;

@Data
public class ReportRequest {
    private Long id; // The user sends this in JSON
}

model file 
package com.tcs.fincore.AsciiGenerationService.Model;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Table(name = "ASCII_CONFIG")
@Data
public class AsciiConfig {

    @Id
    @Column(name = "ID")
    private Long id;

    @Column(name = "REPORT_ID")
    private String reportId;

    @Column(name = "FILE_TYPE")
    private String fileType;

    @Column(name = "INPUT_FILE_NAME")
    private String inputFileName;

    @Column(name = "OUTPUT_FIRST_LINE")
    private String outputFirstLine;

    @Column(name = "OUTPUT_END_LINE")
    private String outputEndLine;

    @Column(name = "INPUT_HEAD_COL")
    private Integer inputHeadCol;

    @Column(name = "INPUT_HEAD_REGEX")
    private String inputHeadRegex;

    @Column(name = "OUTPUT_HEAD_COL_PAD")
    private String outputHeadColPad;

    @Column(name = "AMOUNT_COL_SEQ")
    private String amountColSeq;

    @Column(name = "OUTPUT_AMT_COL_LOGIC")
    private String outputAmtColLogic;

    @Column(name = "OUTPUT_AMT_DECIMAL")
    private String outputAmtDecimal;

    @Column(name = "OUTPUT_AMT_COL_PAD")
    private String outputAmtColPad;

    @Column(name = "OUTPUT_AMT_SIGN")
    private String outputAmtSign;

    @Column(name = "OUTPUT_PER_LINE_HEAD")
    private Integer outputPerLineHead;

    @Column(name = "OUTPUT_INCLUDE_CONDITION")
    private String outputIncludeCondition;
}

repository file
package com.tcs.fincore.AsciiGenerationService.Repository;


import com.tcs.fincore.AsciiGenerationService.Model.AsciiConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AsciiConfigRepository extends JpaRepository<AsciiConfig, Long> {
}

service files 1 asciigenservice 
package com.tcs.fincore.AsciiGenerationService.Service;
import com.tcs.fincore.AsciiGenerationService.DTO.FileJob;
import com.tcs.fincore.AsciiGenerationService.Model.AsciiConfig;
import com.tcs.fincore.AsciiGenerationService.Repository.AsciiConfigRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Service
@Slf4j
public class AsciiGenerationService {

    @Autowired
    private AsciiConfigRepository configRepo;

    @Autowired
    private JobQueueManager jobQueueManager;

    @Value("${app.input.base.path}")
    private String baseDirectoryPath;

    private static final Map<String, Pattern> INPUT_REGEX_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, BatchStats> batchTracker = new ConcurrentHashMap<>();

    private static class BatchStats {
        long startTime;
        AtomicInteger remainingFiles;
        int totalFiles;
        BatchStats(int totalFiles) {
            this.startTime = System.currentTimeMillis();
            this.totalFiles = totalFiles;
            this.remainingFiles = new AtomicInteger(totalFiles);
        }
    }

    // --- 1. THE DISCOVERY PHASE (Unchanged) ---
    public String initiateBatch(Long configId) {
        try {
            AsciiConfig config = configRepo.findById(configId)
                    .orElseThrow(() -> new RuntimeException("Config not found: " + configId));

            String reportId = config.getReportId();
            String searchKeyword = reportId.contains("_") ? reportId.split("_")[0] : reportId;

            Path baseDir = Paths.get(baseDirectoryPath);
            if (!Files.exists(baseDir) || !Files.isDirectory(baseDir)) {
                throw new RuntimeException("Base directory does not exist: " + baseDirectoryPath);
            }

            List<Path> foundFiles = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(baseDir, entry ->
                    Files.isRegularFile(entry) && entry.getFileName().toString().contains(searchKeyword))) {
                for (Path entry : stream) foundFiles.add(entry);
            }

            if (foundFiles.isEmpty()) return "No files found for keyword: " + searchKeyword;

            String batchId = UUID.randomUUID().toString();
            batchTracker.put(batchId, new BatchStats(foundFiles.size()));

            log.info("Batch Started. Batch ID: {} | Total Files: {}", batchId, foundFiles.size());

            for (Path entry : foundFiles) {
                jobQueueManager.submitFileJob(new FileJob(configId, entry, batchId));
            }

            return "Batch Initiated. ID: " + batchId + " | Files: " + foundFiles.size();

        } catch (Exception e) {
            log.error("Error during Batch Initiation", e);
            throw new RuntimeException("Failed to initiate batch: " + e.getMessage());
        }
    }

    // --- 2. THE WORKER PHASE ---
    public void processSingleFile(FileJob job) {
        Path inputPath = job.getFilePath();
        Long id = job.getConfigId();
        String batchId = job.getBatchId();

        try {
            AsciiConfig config = configRepo.findById(id).orElseThrow();

            // --- EDGE CASE FIX: Safe File Name Handling ---
            String originalName = inputPath.getFileName().toString();
            int dotIndex = originalName.lastIndexOf('.');
            String baseName = (dotIndex == -1) ? originalName : originalName.substring(0, dotIndex);

            String outputFileName = "GENERATED_" + config.getReportId() + "_" + baseName + "_" + System.currentTimeMillis() + config.getFileType();
            Path outputPath = inputPath.getParent().resolve(outputFileName);

            // --- STREAMING PROCESSING ---
            try (Stream<String> lines = Files.lines(inputPath);
                 BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

                // Header
                HeaderInfo headerInfo = parseHeaderEfficiently(inputPath);
                String branchPadded = String.format("%5s", headerInfo.branchCode).replace(' ', '0');
                String header = config.getOutputFirstLine() + branchPadded + headerInfo.reportDate + "F";

                writer.write(header);
                writer.newLine();

                List<String> lineBuffer = new ArrayList<>();
                int batchSize = config.getOutputPerLineHead();

                lines.forEach(line -> {
                    try {
                        String processedRecord = transformLine(line, config);
                        if (processedRecord != null) {
                            lineBuffer.add(processedRecord);
                            if (lineBuffer.size() == batchSize) {
                                writer.write(String.join("", lineBuffer));
                                writer.newLine();
                                lineBuffer.clear();
                            }
                        }
                    } catch (Exception e) {
                        log.error("Row error in {}", inputPath.getFileName(), e);
                    }
                });

                if (!lineBuffer.isEmpty()) {
                    writer.write(String.join("", lineBuffer));
                    writer.newLine();
                }

                if (config.getOutputEndLine() != null && !config.getOutputEndLine().isEmpty()) {
                    writer.write(config.getOutputEndLine());
                }
            }

            checkBatchCompletion(batchId);

        } catch (Exception e) {
            log.error("Failed to process file: {}", inputPath, e);
            checkBatchCompletion(batchId);
        }
    }

    private void checkBatchCompletion(String batchId) {
        BatchStats stats = batchTracker.get(batchId);
        if (stats != null) {
            int remaining = stats.remainingFiles.decrementAndGet();
            if (remaining == 0) {
                long totalDuration = System.currentTimeMillis() - stats.startTime;
                log.info("BATCH COMPLETED | ID: {} | Time: {} ms", batchId, totalDuration);
                batchTracker.remove(batchId);
            }
        }
    }

    // --- 3. ROBUST EXTRACT LOGIC (The Core Logic) ---
    private List<ProcessingItem> extractAmountColumns(String[] cols, AsciiConfig config) {
        List<ProcessingItem> items = new ArrayList<>();
        int headIdx = config.getInputHeadCol() - 1;
        String head = (headIdx < cols.length) ? cols[headIdx].trim() : "";

        String seq = config.getAmountColSeq();

        // [3else4] Logic: Skips Zero/Null, Forces Primary Rules
        if (seq.contains("else")) {
            String[] parts = seq.split("else");
            int primaryCol = Integer.parseInt(parts[0].trim()); // Target Rule Set (3)

            for (String part : parts) {
                int currentIdx = Integer.parseInt(part.trim()) - 1;

                // Safe Parse with Bound Check
                BigDecimal val = BigDecimal.ZERO;
                if (currentIdx < cols.length) {
                    val = parseAmount(cols[currentIdx]);
                }

                if (val.compareTo(BigDecimal.ZERO) != 0) {
                    // FOUND IT: Use 'primaryCol' so correct padding/math rules apply
                    items.add(new ProcessingItem(head, val, primaryCol));
                    return items;
                }
            }
            // Fallback: Return 0 for Primary Column
            items.add(new ProcessingItem(head, BigDecimal.ZERO, primaryCol));
        }

        // [3and4] Logic: Keeps specific rules for each column
        else if (seq.contains("and")) {
            String[] parts = seq.split("and");
            for (String part : parts) {
                int idx = Integer.parseInt(part.trim()) - 1;
                items.add(new ProcessingItem(head, parseAmount(cols, idx), idx + 1));
            }
        }

        // Single Column
        else {
            int idx = Integer.parseInt(seq.trim()) - 1;
            items.add(new ProcessingItem(head, parseAmount(cols, idx), idx + 1));
        }
        return items;
    }

    private String transformLine(String line, AsciiConfig config) {
        String[] columns = line.split("\\|", -1);
        if (!isValidRow(columns, config)) return null;

        List<ProcessingItem> itemsToProcess = extractAmountColumns(columns, config);

        StringBuilder amountsOnlyBuilder = new StringBuilder();
        boolean hasValidContent = false;

        for (ProcessingItem item : itemsToProcess) {
            BigDecimal finalAmt = RuleParser.applyMathLogic(item.amount, item.head, item.colIndex, config.getOutputAmtColLogic());
            finalAmt = RuleParser.applyDecimalLogic(finalAmt, item.head, item.colIndex, config.getOutputAmtDecimal());

            if (RuleParser.shouldIncludeValue(finalAmt, item.head, item.colIndex, config.getOutputIncludeCondition())) {
                String amountStr = RuleParser.applyAmountPadding(finalAmt, item.head, item.colIndex, config.getOutputAmtColPad());
                String signedAmt = RuleParser.applySign(amountStr, finalAmt, item.head, item.colIndex, config.getOutputAmtSign());
                amountsOnlyBuilder.append(signedAmt);
                hasValidContent = true;
            }
        }

        if (hasValidContent) {
            String headPadded = RuleParser.applyHeadPadding(itemsToProcess.get(0).head, config.getOutputHeadColPad());
            return headPadded + amountsOnlyBuilder.toString();
        }
        return null;
    }

    // --- Standard Helpers  ---
    private BigDecimal parseAmount(String[] cols, int idx) {
        if (idx >= cols.length) return BigDecimal.ZERO;
        return parseAmount(cols[idx]);
    }
    private BigDecimal parseAmount(String val) {
        try {
            if(val == null || val.trim().isEmpty()) return BigDecimal.ZERO;
            // Robust parsing: remove commas, trim spaces
            return new BigDecimal(val.trim().replace(",", ""));
        } catch (Exception e) { return BigDecimal.ZERO; }
    }

    private HeaderInfo parseHeaderEfficiently(Path path) {
        HeaderInfo info = new HeaderInfo();
        info.branchCode = "00000"; info.reportDate = "01012026";
        Pattern branchPattern = Pattern.compile("(?i)(?:BRANCH|Branch)\\s*::\\s*(\\d+)");
        Pattern datePattern1 = Pattern.compile("REPORT DATE\\s*::\\s*(\\S+)");
        Pattern datePattern2 = Pattern.compile("Current Year Date\\s*::\\s*(\\S+)");

        try (Stream<String> stream = Files.lines(path).limit(50)) {
            stream.forEach(line -> {
                Matcher mBranch = branchPattern.matcher(line);
                if (mBranch.find()) info.branchCode = mBranch.group(1);
                Matcher mDate1 = datePattern1.matcher(line);
                if (mDate1.find()) info.reportDate = normalizeDate(mDate1.group(1));
                Matcher mDate2 = datePattern2.matcher(line);
                if (mDate2.find()) info.reportDate = normalizeDate(mDate2.group(1));
            });
        } catch (IOException e) { log.warn("Header read error", e); }
        return info;
    }


    private boolean isValidRow(String[] cols, AsciiConfig config) {
        if (cols.length < 2) return false;
        int idx = config.getInputHeadCol() - 1;
        if (cols.length <= idx) return false;
        String head = cols[idx].trim();
        if (head.isEmpty()) return false;
        Pattern p = INPUT_REGEX_CACHE.computeIfAbsent(config.getInputHeadRegex(), Pattern::compile);
        return p.matcher(head).lookingAt();
    }

    private String normalizeDate(String dateStr) {
        if (dateStr == null) return "01012026";
        List<DateTimeFormatter> formatters = Arrays.asList(
                DateTimeFormatter.ofPattern("dd-MM-yyyy"), DateTimeFormatter.ofPattern("dd/MM/yyyy"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd"), DateTimeFormatter.ofPattern("dd.MM.yyyy"),
                DateTimeFormatter.ofPattern("MM-dd-yyyy")
        );
        for (DateTimeFormatter formatter : formatters) {
            try { return LocalDate.parse(dateStr, formatter).format(DateTimeFormatter.ofPattern("ddMMyyyy")); }
            catch (DateTimeParseException e) {}
        }
        if (dateStr.contains("-") && dateStr.split("-").length == 3) {
            String[] parts = dateStr.split("-");
            if (parts[0].length() == 4) return parts[2] + parts[1] + parts[0];
        }
        return "01012026";
    }

    private static class ProcessingItem {
        String head; BigDecimal amount; int colIndex;
        ProcessingItem(String h, BigDecimal a, int c) { head=h; amount=a; colIndex=c; }
    }
    private static class HeaderInfo { String branchCode; String reportDate; }
}

file 2 job queue manager
package com.tcs.fincore.AsciiGenerationService.Service;


import com.tcs.fincore.AsciiGenerationService.DTO.FileJob;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy; // <--- IMPORT THIS
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Service
@Slf4j
public class JobQueueManager {

    private final AsciiGenerationService asciiService;

    // Use Constructor Injection with @Lazy to break the cycle
    @Autowired
    public JobQueueManager(@Lazy AsciiGenerationService asciiService) {
        this.asciiService = asciiService;
    }

    // The Queue: Holds 'FileJob' (Config ID + Specific File Path)
    private final BlockingQueue<FileJob> jobQueue = new LinkedBlockingQueue<>();

    // 10 Threads processing files in parallel
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    private volatile boolean running = true;

    @PostConstruct
    public void startWorkers() {
        Thread dispatcher = new Thread(() -> {
            while (running) {
                try {
                    FileJob job = jobQueue.take();
                    executorService.submit(() -> asciiService.processSingleFile(job));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        dispatcher.setName("Job-Dispatcher");
        dispatcher.start();
        log.info("Batch Processor Started. Waiting for jobs...");
    }

    public void submitFileJob(FileJob job) {
        jobQueue.offer(job);
    }

    public int getQueueSize() {
        return jobQueue.size();
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        executorService.shutdown();
    }
}

file 3 rule parser
package com.tcs.fincore.AsciiGenerationService.Service;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RuleParser {

    // Regex to capture: ["Content"] [Col] [Logic]
    private static final Pattern RULE_PATTERN = Pattern.compile("\\[\"(.*?)\"\\]\\[(.*?)\\]\\[(.*?)\\]");

    // CPU OPTIMIZATION: Cache compiled regex patterns to avoid re-compiling millions of times
    private static final Map<String, Pattern> REGEX_CACHE = new ConcurrentHashMap<>();

    // Helper to get cached pattern
    private static Pattern getCachedPattern(String regex) {
        return REGEX_CACHE.computeIfAbsent(regex, Pattern::compile);
    }

    // --- 1. MATH LOGIC (CHAINED) ---
    public static BigDecimal applyMathLogic(BigDecimal amount, String head, int colIdx, String logicConfig) {
        if (logicConfig == null) return amount;

        Matcher m = RULE_PATTERN.matcher(logicConfig);
        while (m.find()) {
            String headRegex = m.group(1);
            String colReq = m.group(2);
            String logic = m.group(3);

            boolean colMatch = colReq.isEmpty() || colReq.equals(String.valueOf(colIdx));
            // OPTIMIZED: Use Cache
            boolean headMatch = getCachedPattern(headRegex).matcher(head).lookingAt();

            if (colMatch && headMatch) {
                if (logic.contains("/")) {
                    BigDecimal divisor = new BigDecimal(logic.replaceAll("[^0-9.]", ""));
                    if (divisor.compareTo(BigDecimal.ZERO) != 0) {
                        amount = amount.divide(divisor, 6, RoundingMode.HALF_UP);
                    }
                } else if (logic.contains("*")) {
                    BigDecimal multiplier = new BigDecimal(logic.replace("*", "").replace("(", "").replace(")", ""));
                    amount = amount.multiply(multiplier);
                } else if (logic.contains("+")) {
                    BigDecimal add = new BigDecimal(logic.replace("+", "").replace("(", "").replace(")", ""));
                    amount = amount.add(add);
                } else if (logic.contains("-")) {
                    BigDecimal sub = new BigDecimal(logic.replace("-", "").replace("(", "").replace(")", ""));
                    amount = amount.subtract(sub);
                }
            }
        }
        return amount;
    }

    // --- 2. DECIMAL LOGIC (CHAINED) ---
    public static BigDecimal applyDecimalLogic(BigDecimal amount, String head, int colIdx, String decimalConfig) {
        if (decimalConfig == null) return amount;

        Matcher m = RULE_PATTERN.matcher(decimalConfig);
        while (m.find()) {
            String headRegex = m.group(1);
            String colReq = m.group(2);
            String rule = m.group(3);

            boolean colMatch = colReq.isEmpty() || colReq.equals(String.valueOf(colIdx));
            boolean headMatch = getCachedPattern(headRegex).matcher(head).lookingAt();

            if (colMatch && headMatch) {
                if (rule.equalsIgnoreCase("floor")) {
                    amount = amount.setScale(0, RoundingMode.FLOOR);
                } else if (rule.equalsIgnoreCase("ceil")) {
                    amount = amount.setScale(0, RoundingMode.CEILING);
                } else if (rule.equalsIgnoreCase("round")) {
                    amount = amount.setScale(0, RoundingMode.HALF_UP);
                } else if (rule.equalsIgnoreCase("down")) {
                    amount = amount.setScale(0, RoundingMode.DOWN);
                }
                // Logic: Remove Decimal (123.29 -> 12329)
                else if (rule.equalsIgnoreCase("remove_decimal") || rule.equalsIgnoreCase("remove")) {
                    amount = amount.movePointRight(amount.scale());
                }
                else {
                    try {
                        int scale = Integer.parseInt(rule);
                        amount = amount.setScale(scale, RoundingMode.HALF_UP);
                    } catch (NumberFormatException e) {}
                }
            }
        }
        return amount;
    }

    // --- 3. PADDING LOGIC (CHAINED) ---
    public static String applyAmountPadding(BigDecimal amount, String head, int colIdx, String padConfig) {
        BigDecimal absAmount = amount.abs();
        String currentStr = absAmount.toPlainString();

        if (padConfig == null) return currentStr;

        Matcher m = RULE_PATTERN.matcher(padConfig);
        while (m.find()) {
            String headRegex = m.group(1);
            String colReq = m.group(2);
            String rule = m.group(3);

            boolean colMatch = colReq.isEmpty() || colReq.equals(String.valueOf(colIdx));
            boolean headMatch = getCachedPattern(headRegex).matcher(head).lookingAt();

            if (colMatch && headMatch) {
                if (rule.contains(",")) {
                    String[] parts = rule.split(",");
                    int totalLen = Integer.parseInt(parts[0]);
                    int decimalLen = Integer.parseInt(parts[1]);

                    BigDecimal scaled = absAmount.setScale(decimalLen, RoundingMode.HALF_UP);
                    currentStr = padLeft(scaled.toPlainString(), totalLen, '0');
                } else {
                    int totalLen = Integer.parseInt(rule);
                    currentStr = padLeft(currentStr, totalLen, '0');
                }
            }
        }
        return currentStr;
    }

    // --- 4. SIGN LOGIC (CHAINED & TRIMMED) ---
    public static String applySign(String fmtAmt, BigDecimal origAmt, String head, int colIdx, String signConfig) {
        String currentStr = fmtAmt;

        if (signConfig == null || signConfig.trim().isEmpty()) {
            currentStr = currentStr.replace("+", "").replace("-", "");
            return (origAmt.compareTo(BigDecimal.ZERO) < 0) ? currentStr + "-" : currentStr;
        }

        Matcher m = RULE_PATTERN.matcher(signConfig);
        boolean anyRuleApplied = false;

        while (m.find()) {
            String headRegex = m.group(1);
            String colReq = m.group(2);
            String rule = m.group(3) != null ? m.group(3).trim() : "";

            boolean colMatch = colReq.isEmpty() || colReq.equals(String.valueOf(colIdx));
            boolean headMatch = getCachedPattern(headRegex).matcher(head).lookingAt();

            if (colMatch && headMatch) {
                anyRuleApplied = true;
                currentStr = currentStr.replace("+", "").replace("-", "");

                if ("NO_SIGN".equalsIgnoreCase(rule)) {
                    // Do nothing
                } else if ("SHOW_SIGN".equalsIgnoreCase(rule)) {
                    if (origAmt.compareTo(BigDecimal.ZERO) >= 0) {
                        currentStr = currentStr + "+";
                    } else {
                        currentStr = currentStr + "-";
                    }
                } else if ("POS_0".equalsIgnoreCase(rule)) {
                    if (origAmt.compareTo(BigDecimal.ZERO) >= 0) {
                        currentStr = currentStr + "0";
                    } else {
                        currentStr = currentStr + "-";
                    }
                }
            }
        }

        if (!anyRuleApplied) {
            currentStr = currentStr.replace("+", "").replace("-", "");
            return (origAmt.compareTo(BigDecimal.ZERO) < 0) ? currentStr + "-" : currentStr;
        }

        return currentStr;
    }

    // --- 5. INCLUSION LOGIC (CHAINED) ---
    public static boolean shouldIncludeValue(BigDecimal amount, String head, int colIdx, String includeConfig) {
        if (includeConfig == null) return true;

        boolean include = true;
        Matcher m = RULE_PATTERN.matcher(includeConfig);

        while (m.find()) {
            String headRegex = m.group(1);
            String colReq = m.group(2);
            String rule = m.group(3);

            boolean colMatch = colReq.isEmpty() || colReq.equals(String.valueOf(colIdx));
            boolean headMatch = getCachedPattern(headRegex).matcher(head).lookingAt();

            if (colMatch && headMatch) {
                if ("ALWAYS".equalsIgnoreCase(rule)) { /* Pass */ }
                else if (rule.equals(">0") && amount.compareTo(BigDecimal.ZERO) <= 0) include = false;
                else if (rule.equals("<0") && amount.compareTo(BigDecimal.ZERO) >= 0) include = false;
                else if ((rule.equals("=0") || rule.equals("0")) && amount.compareTo(BigDecimal.ZERO) != 0) include = false;
                else if (rule.equals("!=0") && amount.compareTo(BigDecimal.ZERO) == 0) include = false;
            }
        }
        return include;
    }

    // --- HEAD PADDING ---
    public static String applyHeadPadding(String head, String padConfig) {
        if (padConfig == null || padConfig.length() < 3) return head;
        int len = Integer.parseInt(padConfig.substring(0, padConfig.length() - 2));
        char dir = padConfig.charAt(padConfig.length() - 2);
        char pad = padConfig.endsWith("\" \"") ? ' ' : padConfig.charAt(padConfig.length() - 1);
        return dir == 'L' ? padLeft(head, len, pad) : padRight(head, len, pad);
    }

    private static String padLeft(String str, int len, char pad) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() + str.length() < len) sb.append(pad);
        return sb.append(str).toString();
    }
    private static String padRight(String str, int len, char pad) {
        StringBuilder sb = new StringBuilder(str);
        while (sb.length() < len) sb.append(pad);
        return sb.toString();
    }
}

application file 
package com.tcs.fincore.AsciiGenerationService;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AsciiGenerationServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(AsciiGenerationServiceApplication.class, args);
	}

}

resource file 
spring.application.name=AsciiGenerationService

#spring.datasource.url=jdbc:oracle:thin:@10.191.216.58:1522:crsprod
#spring.datasource.driver-class-name=oracle.jdbc.OracleDriver
#spring.datasource.username=ftwoahm
#spring.datasource.password=Password@123


spring.datasource.url=jdbc:oracle:thin:@10.177.103.192:1523/fincorepdb1
spring.datasource.username=fincore
spring.datasource.password=Password#1234
spring.datasource.driver-class-name=oracle.jdbc.OracleDriver
server.port=8090


# --- Redis Configuration ---
spring.data.redis.host=10.0.17.242
spring.data.redis.port=6379
spring.cache.type=redis


app.input.base.path=C:/Users/v1021253/Documents/Files/input/

