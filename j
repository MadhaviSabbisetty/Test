BPEP10000131032026F
01099000000019076805923+000000000001340788+
01001000000000229837700+000000000000000000+
01002000000003311293000+000000000000194568+
01003000000000000000000+000000000000000000+
01004000000015535675223+000000000001146220+
01005000000000000000000+000000000000000000+
01006000000000000000000+000000000000000000+
01019000000019076805923+000000000001340788+
01020000000000000000000+000000000000000000+
01039000000000000000000+000000000000000000+
01026000000000000000000+000000000000000000+
01027000000000000000000+000000000000000000+
01028000000000000000000+000000000000000000+
01029000000000000000000+000000000000000000+
01030000000000000000000+000000000000000000+
01031000000000000000000+000000000000000000+
01032000000000000000000+000000000000000000+
01033000000000000000000+000000000000000000+
01059000000000000000000+000000000000000000+
01041000000000000000000+000000000000000000+
01042000000000000000000+000000000000000000+
01049000000000000000000+000000000000000000+
02099000000161344095719+000000092065579381+
02001000000157365099853+000000091330229410+
02002000000000005153401+000000000000000000+
02003000000000000000000+000000000000000000+
02004000000000178713654+000000000000000000+








private String transformLine(String line, AsciiConfig config) {

    String[] cols = line.split("\\|", -1);

    // Skip header/invalid lines
    if (cols.length < 5) return null;

    String head = cols[3].trim();   // FIELD NO (01099)
    BigDecimal prevAmt = parseAmount(cols[0]); // Previous
    BigDecimal currAmt = parseAmount(cols[4]); // Current

    // Skip non-data rows
    if (!head.matches("\\d+")) return null;

    // Format amounts like your ASCII
    String prev = formatAmount(prevAmt);
    String curr = formatAmount(currAmt);

    return head + prev + curr;
}


private String formatAmount(BigDecimal amt) {

    if (amt == null) amt = BigDecimal.ZERO;

    boolean negative = amt.compareTo(BigDecimal.ZERO) < 0;

    amt = amt.abs().setScale(2, BigDecimal.ROUND_HALF_UP);

    String value = amt.movePointRight(2).toPlainString(); // remove decimal

    // pad to 23 digits
    String padded = String.format("%023d", new java.math.BigInteger(value));

    return padded + (negative ? "-" : "+");
}






[root@fcdevhdfsname media]# hdfs dfs -cat /reports/2026-03-31/pnl_report/pnl_report_31032026.psv
================================================================================
                              STATE BANK OF INDIA
================================================================================

                                 PROFIT & LOSS

Branch :: BANK LEVEL REPORT
RUN DATE :: 2026-04-01 10:46:55
Report Date :: 2026-03-31
Previous Year Date :: 2025-03-31
CURRENCY :: 000 INDIAN RUPEE

================================================================================
Previous Year Balance (Rs.Ps)|Sr. No|DESCRIPTION|FIELD No.|Current Year Balance (Rs.Ps)
================================================================================
||INCOME||
0.00|1|INTEREST RECEIVED (A+B)|01099|0.00
0.00|i.|INTEREST RECEIVED ON TERM LOANS|01001|0.00
0.00|ii.|INTEREST RECEIVED ON DEMAND LOANS(INCLUDING INTEREST ON WCDL)|01002|0.00
0.00|iii.|INT RECEIVED ON CASH CREDITS(INCLUDING INT REALISED IN RECALLED ASSET)|01003|0.00
0.00|iv.|INTEREST RECEIVED ON OVERDRAFT(INCLUDING INT REALISED IN RECALED ASST)|01004|0.00
0.00|v.|DISCOUNT (LESS REBATE IF ANY )|01005|0.00
0.00|vi.|INTEREST ON BILLS/CHEQUES PURCHASED|01006|0.00
0.00|A.|TOTAL INTEREST/DISCOUNT ON ADVANCES/BILLS(TOTAL i-vi)(EXCL.C.O.INTT)|01019|0.00
0.00|a.|INTEERST/DISCOUNT/DIVIDEND ON INVESTMENTS (AFTER ADJUSTMENT OF LOSS OR GAIN ON REVALUATION OF HTM CATEGORY) (Only GMU,IBG & CAO)|01020|0.00
0.00|b.|INTEREST ON DEPOSITS WITH RBI AND OTHER BANK (i - viii)|01039|0.00
0.00|i.|INTEREST ON BALANCE WITH RBI|01026|0.00



previous code 
// package com.tcs.fincore.AsciiGenerationService.Service;
// import com.tcs.fincore.AsciiGenerationService.DTO.FileJob;
// import com.tcs.fincore.AsciiGenerationService.Model.AsciiConfig;
// import com.tcs.fincore.AsciiGenerationService.Repository.AsciiConfigRepository;
// import lombok.extern.slf4j.Slf4j;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.stereotype.Service;
// import java.io.BufferedWriter;
// import java.io.IOException;
// import java.math.BigDecimal;
// import java.nio.file.DirectoryStream;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.nio.file.Paths;
// import java.time.LocalDate;
// import java.time.format.DateTimeFormatter;
// import java.time.format.DateTimeParseException;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.List;
// import java.util.Map;
// import java.util.UUID;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.atomic.AtomicInteger;
// import java.util.regex.Matcher;
// import java.util.regex.Pattern;
// import java.util.stream.Stream;

// @Service
// @Slf4j
// public class AsciiGenerationService {

//     @Autowired
//     private AsciiConfigRepository configRepo;

//     @Autowired
//     private JobQueueManager jobQueueManager;

//     @Value("${app.input.base.path}")
//     private String baseDirectoryPath;

//     private static final Map<String, Pattern> INPUT_REGEX_CACHE = new ConcurrentHashMap<>();
//     private static final Map<String, BatchStats> batchTracker = new ConcurrentHashMap<>();

//     private static class BatchStats {
//         long startTime;
//         AtomicInteger remainingFiles;
//         int totalFiles;
//         BatchStats(int totalFiles) {
//             this.startTime = System.currentTimeMillis();
//             this.totalFiles = totalFiles;
//             this.remainingFiles = new AtomicInteger(totalFiles);
//         }
//     }

//     // --- 1. THE DISCOVERY PHASE (Unchanged) ---
//     public String initiateBatch(Long configId) {
//         try {
//             AsciiConfig config = configRepo.findById(configId)
//                     .orElseThrow(() -> new RuntimeException("Config not found: " + configId));

//             String reportId = config.getReportId();
//             String searchKeyword = reportId.contains("_") ? reportId.split("_")[0] : reportId;

//             Path baseDir = Paths.get(baseDirectoryPath);
//             if (!Files.exists(baseDir) || !Files.isDirectory(baseDir)) {
//                 throw new RuntimeException("Base directory does not exist: " + baseDirectoryPath);
//             }

//             List<Path> foundFiles = new ArrayList<>();
//             try (DirectoryStream<Path> stream = Files.newDirectoryStream(baseDir, entry ->
//                     Files.isRegularFile(entry) && entry.getFileName().toString().contains(searchKeyword))) {
//                 for (Path entry : stream) foundFiles.add(entry);
//             }

//             if (foundFiles.isEmpty()) return "No files found for keyword: " + searchKeyword;

//             String batchId = UUID.randomUUID().toString();
//             batchTracker.put(batchId, new BatchStats(foundFiles.size()));

//             log.info("Batch Started. Batch ID: {} | Total Files: {}", batchId, foundFiles.size());

//             for (Path entry : foundFiles) {
//                 jobQueueManager.submitFileJob(new FileJob(configId, entry, batchId));
//             }

//             return "Batch Initiated. ID: " + batchId + " | Files: " + foundFiles.size();

//         } catch (Exception e) {
//             log.error("Error during Batch Initiation", e);
//             throw new RuntimeException("Failed to initiate batch: " + e.getMessage());
//         }
//     }

//     // --- 2. THE WORKER PHASE ---
//     public void processSingleFile(FileJob job) {
//         Path inputPath = job.getFilePath();
//         Long id = job.getConfigId();
//         String batchId = job.getBatchId();

//         try {
//             AsciiConfig config = configRepo.findById(id).orElseThrow();

//             // --- EDGE CASE FIX: Safe File Name Handling ---
//             String originalName = inputPath.getFileName().toString();
//             int dotIndex = originalName.lastIndexOf('.');
//             String baseName = (dotIndex == -1) ? originalName : originalName.substring(0, dotIndex);

//             String outputFileName = "GENERATED_" + config.getReportId() + "_" + baseName + "_" + System.currentTimeMillis() + config.getFileType();
//             Path outputPath = inputPath.getParent().resolve(outputFileName);

//             // --- STREAMING PROCESSING ---
//             try (Stream<String> lines = Files.lines(inputPath);
//                  BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

//                 // Header
//                 HeaderInfo headerInfo = parseHeaderEfficiently(inputPath);
//                 String branchPadded = String.format("%5s", headerInfo.branchCode).replace(' ', '0');
//                 String header = config.getOutputFirstLine() + branchPadded + headerInfo.reportDate + "F";

//                 writer.write(header);
//                 writer.newLine();

//                 List<String> lineBuffer = new ArrayList<>();
//                 int batchSize = config.getOutputPerLineHead();

//                 lines.forEach(line -> {
//                     try {
//                         String processedRecord = transformLine(line, config);
//                         if (processedRecord != null) {
//                             lineBuffer.add(processedRecord);
//                             if (lineBuffer.size() == batchSize) {
//                                 writer.write(String.join("", lineBuffer));
//                                 writer.newLine();
//                                 lineBuffer.clear();
//                             }
//                         }
//                     } catch (Exception e) {
//                         log.error("Row error in {}", inputPath.getFileName(), e);
//                     }
//                 });

//                 if (!lineBuffer.isEmpty()) {
//                     writer.write(String.join("", lineBuffer));
//                     writer.newLine();
//                 }

//                 if (config.getOutputEndLine() != null && !config.getOutputEndLine().isEmpty()) {
//                     writer.write(config.getOutputEndLine());
//                 }
//             }

//             checkBatchCompletion(batchId);

//         } catch (Exception e) {
//             log.error("Failed to process file: {}", inputPath, e);
//             checkBatchCompletion(batchId);
//         }
//     }

//     private void checkBatchCompletion(String batchId) {
//         BatchStats stats = batchTracker.get(batchId);
//         if (stats != null) {
//             int remaining = stats.remainingFiles.decrementAndGet();
//             if (remaining == 0) {
//                 long totalDuration = System.currentTimeMillis() - stats.startTime;
//                 log.info("BATCH COMPLETED | ID: {} | Time: {} ms", batchId, totalDuration);
//                 batchTracker.remove(batchId);
//             }
//         }
//     }

//     // --- 3. ROBUST EXTRACT LOGIC (The Core Logic) ---
//     private List<ProcessingItem> extractAmountColumns(String[] cols, AsciiConfig config) {
//         List<ProcessingItem> items = new ArrayList<>();
//         int headIdx = config.getInputHeadCol() - 1;
//         String head = (headIdx < cols.length) ? cols[headIdx].trim() : "";

//         String seq = config.getAmountColSeq();

//         // [3else4] Logic: Skips Zero/Null, Forces Primary Rules
//         if (seq.contains("else")) {
//             String[] parts = seq.split("else");
//             int primaryCol = Integer.parseInt(parts[0].trim()); // Target Rule Set (3)

//             for (String part : parts) {
//                 int currentIdx = Integer.parseInt(part.trim()) - 1;

//                 // Safe Parse with Bound Check
//                 BigDecimal val = BigDecimal.ZERO;
//                 if (currentIdx < cols.length) {
//                     val = parseAmount(cols[currentIdx]);
//                 }

//                 if (val.compareTo(BigDecimal.ZERO) != 0) {
//                     // FOUND IT: Use 'primaryCol' so correct padding/math rules apply
//                     items.add(new ProcessingItem(head, val, primaryCol));
//                     return items;
//                 }
//             }
//             // Fallback: Return 0 for Primary Column
//             items.add(new ProcessingItem(head, BigDecimal.ZERO, primaryCol));
//         }

//         // [3and4] Logic: Keeps specific rules for each column
//         else if (seq.contains("and")) {
//             String[] parts = seq.split("and");
//             for (String part : parts) {
//                 int idx = Integer.parseInt(part.trim()) - 1;
//                 items.add(new ProcessingItem(head, parseAmount(cols, idx), idx + 1));
//             }
//         }

//         // Single Column
//         else {
//             int idx = Integer.parseInt(seq.trim()) - 1;
//             items.add(new ProcessingItem(head, parseAmount(cols, idx), idx + 1));
//         }
//         return items;
//     }

//     private String transformLine(String line, AsciiConfig config) {
//         String[] columns = line.split("\\|", -1);
//         if (!isValidRow(columns, config)) return null;

//         List<ProcessingItem> itemsToProcess = extractAmountColumns(columns, config);

//         StringBuilder amountsOnlyBuilder = new StringBuilder();
//         boolean hasValidContent = false;

//         for (ProcessingItem item : itemsToProcess) {
//             BigDecimal finalAmt = RuleParser.applyMathLogic(item.amount, item.head, item.colIndex, config.getOutputAmtColLogic());
//             finalAmt = RuleParser.applyDecimalLogic(finalAmt, item.head, item.colIndex, config.getOutputAmtDecimal());

//             if (RuleParser.shouldIncludeValue(finalAmt, item.head, item.colIndex, config.getOutputIncludeCondition())) {
//                 String amountStr = RuleParser.applyAmountPadding(finalAmt, item.head, item.colIndex, config.getOutputAmtColPad());
//                 String signedAmt = RuleParser.applySign(amountStr, finalAmt, item.head, item.colIndex, config.getOutputAmtSign());
//                 amountsOnlyBuilder.append(signedAmt);
//                 hasValidContent = true;
//             }
//         }

//         if (hasValidContent) {
//             String headPadded = RuleParser.applyHeadPadding(itemsToProcess.get(0).head, config.getOutputHeadColPad());
//             return headPadded + amountsOnlyBuilder.toString();
//         }
//         return null;
//     }

//     // --- Standard Helpers  ---
//     private BigDecimal parseAmount(String[] cols, int idx) {
//         if (idx >= cols.length) return BigDecimal.ZERO;
//         return parseAmount(cols[idx]);
//     }
//     private BigDecimal parseAmount(String val) {
//         try {
//             if(val == null || val.trim().isEmpty()) return BigDecimal.ZERO;
//             // Robust parsing: remove commas, trim spaces
//             return new BigDecimal(val.trim().replace(",", ""));
//         } catch (Exception e) { return BigDecimal.ZERO; }
//     }

//     private HeaderInfo parseHeaderEfficiently(Path path) {
//         HeaderInfo info = new HeaderInfo();
//         info.branchCode = "00000"; info.reportDate = "01012026";
//         Pattern branchPattern = Pattern.compile("(?i)(?:BRANCH|Branch)\\s*::\\s*(\\d+)");
//         Pattern datePattern1 = Pattern.compile("REPORT DATE\\s*::\\s*(\\S+)");
//         Pattern datePattern2 = Pattern.compile("Current Year Date\\s*::\\s*(\\S+)");

//         try (Stream<String> stream = Files.lines(path).limit(50)) {
//             stream.forEach(line -> {
//                 Matcher mBranch = branchPattern.matcher(line);
//                 if (mBranch.find()) info.branchCode = mBranch.group(1);
//                 Matcher mDate1 = datePattern1.matcher(line);
//                 if (mDate1.find()) info.reportDate = normalizeDate(mDate1.group(1));
//                 Matcher mDate2 = datePattern2.matcher(line);
//                 if (mDate2.find()) info.reportDate = normalizeDate(mDate2.group(1));
//             });
//         } catch (IOException e) { log.warn("Header read error", e); }
//         return info;
//     }


//     private boolean isValidRow(String[] cols, AsciiConfig config) {
//         if (cols.length < 2) return false;
//         int idx = config.getInputHeadCol() - 1;
//         if (cols.length <= idx) return false;
//         String head = cols[idx].trim();
//         if (head.isEmpty()) return false;
//         Pattern p = INPUT_REGEX_CACHE.computeIfAbsent(config.getInputHeadRegex(), Pattern::compile);
//         return p.matcher(head).lookingAt();
//     }

//     private String normalizeDate(String dateStr) {
//         if (dateStr == null) return "01012026";
//         List<DateTimeFormatter> formatters = Arrays.asList(
//                 DateTimeFormatter.ofPattern("dd-MM-yyyy"), DateTimeFormatter.ofPattern("dd/MM/yyyy"),
//                 DateTimeFormatter.ofPattern("yyyy-MM-dd"), DateTimeFormatter.ofPattern("dd.MM.yyyy"),
//                 DateTimeFormatter.ofPattern("MM-dd-yyyy")
//         );
//         for (DateTimeFormatter formatter : formatters) {
//             try { return LocalDate.parse(dateStr, formatter).format(DateTimeFormatter.ofPattern("ddMMyyyy")); }
//             catch (DateTimeParseException e) {}
//         }
//         if (dateStr.contains("-") && dateStr.split("-").length == 3) {
//             String[] parts = dateStr.split("-");
//             if (parts[0].length() == 4) return parts[2] + parts[1] + parts[0];
//         }
//         return "01012026";
//     }

//     private static class ProcessingItem {
//         String head; BigDecimal amount; int colIndex;
//         ProcessingItem(String h, BigDecimal a, int c) { head=h; amount=a; colIndex=c; }
//     }
//     private static class HeaderInfo { String branchCode; String reportDate; }
// }




current code 

package com.tcs.fincore.AsciiGenerationService.Service;

import com.tcs.fincore.AsciiGenerationService.DTO.FileJob;
import com.tcs.fincore.AsciiGenerationService.Model.AsciiConfig;
import com.tcs.fincore.AsciiGenerationService.Repository.AsciiConfigRepository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.math.BigDecimal;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class AsciiGenerationService {

    @Autowired
    private AsciiConfigRepository configRepo;

    @Autowired
    private JobQueueManager jobQueueManager;

    @Autowired
    private HdfsService hdfsService;

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

    // =========================
    // 1. INITIATE BATCH (HDFS)
    // =========================
    public String initiateBatch(Long configId) {
        try {
            AsciiConfig config = configRepo.findById(configId)
                    .orElseThrow(() -> new RuntimeException("Config not found: " + configId));

            String reportId = config.getReportId();
            String keyword = reportId.contains("_") ? reportId.split("_")[0] : reportId;

            FileSystem fs = hdfsService.getFs();
            // Path baseDir = new Path("/reports/2025-12-14/nwsa_report");

            Path baseDir = new Path("/reports/2026-03-31-pnl_report");

            RemoteIterator<LocatedFileStatus> files = fs.listFiles(baseDir, true);

            List<Path> foundFiles = new ArrayList<>();

            while (files.hasNext()) {
                LocatedFileStatus file = files.next();
                String fileName = file.getPath().getName();

                log.info("------------------Checking the files: {}",file.getPath());

                if (fileName.contains(keyword) && fileName.endsWith(".psv")) {
                    foundFiles.add(file.getPath());
                }
            }

            if (foundFiles.isEmpty()) {
                return "No files found for keyword: " + keyword;
            }

            String batchId = UUID.randomUUID().toString();
            batchTracker.put(batchId, new BatchStats(foundFiles.size()));

            log.info("-------------Batch Started | ID: {} | Files: {}", batchId, foundFiles.size());

            for (Path file : foundFiles) {
                jobQueueManager.submitFileJob(new FileJob(configId, file, batchId));
            }

            return "Batch Initiated: " + batchId;

        } catch (Exception e) {
            log.error("--------Batch initiation failed", e);
            throw new RuntimeException(e);
        }
    }

    // =========================
    // 2. PROCESS FILE (HDFS)
    // =========================
    public void processSingleFile(FileJob job) {

        Path inputPath = job.getFilePath();
        Long configId = job.getConfigId();
        String batchId = job.getBatchId();

        try {
            FileSystem fs = hdfsService.getFs();
            AsciiConfig config = configRepo.findById(configId).orElseThrow();
            String fullPath=inputPath.toString();
            log.info("-----------------------------------INPUT PATH: {}",fullPath);

            String[] parts =fullPath.split("/");
            String date=parts[4];
            String reportFolder=config.getReportId().toLowerCase().trim();
            String reportType=reportFolder.contains("_")
                                ? reportFolder.split("_")[0]
                                : reportFolder;

            // // ===== SAFE PATH EXTRACTION =====
            // Path parent = inputPath.getParent();              // branch OR filename
            // Path grandParent = parent.getParent();            // filename
            // Path greatGrandParent = grandParent.getParent();  // date

            // String filename = grandParent.getName();
            // String date = greatGrandParent.getName();

            // ===== OUTPUT DIRECTORY =====
            String outputDirStr = "/reports/" + date + "/ascii_files/" + reportType + "/";
            Path outputDir = new Path(outputDirStr);

            log.info("----------------------OUTPUT PATH: {}",outputDirStr);

            if (!fs.exists(outputDir)) {
                fs.mkdirs(outputDir);
            }

            String outputFileName =
                    // "GENERATED_" + config.getReportId() + "_" +
                    "GENERATED_" + reportFolder + "_" +
                    System.currentTimeMillis() + config.getFileType();

            Path outputPath = new Path(outputDir, outputFileName);

            log.info("---------------------Processing: {}", inputPath);
            log.info("------------------Output: {}", outputPath);

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fs.open(inputPath)));

            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fs.create(outputPath)));

            // ===== HEADER =====
            HeaderInfo headerInfo = parseHeaderEfficiently(fs, inputPath);

            String branchPadded = String.format("%5s", headerInfo.branchCode).replace(' ', '0');

            String header =
                    config.getOutputFirstLine() +
                    branchPadded +
                    headerInfo.reportDate +
                    "F";

            writer.write(header);
            writer.newLine();

            // ===== PROCESS DATA =====
            List<String> buffer = new ArrayList<>();
            int batchSize = config.getOutputPerLineHead();

            String line;

            while ((line = reader.readLine()) != null) {
                try {
                    String processed = transformLine(line, config);

                    if (processed != null) {
                        buffer.add(processed);

                        if (buffer.size() == batchSize) {
                            writer.write(String.join("", buffer));
                            writer.newLine();
                            buffer.clear();
                        }
                    }

                } catch (Exception e) {
                    log.error("-----------Row error in {}", inputPath, e);
                }
            }

            if (!buffer.isEmpty()) {
                writer.write(String.join("", buffer));
                writer.newLine();
            }

            // ===== FOOTER =====
            if (config.getOutputEndLine() != null) {
                writer.write(config.getOutputEndLine());
            }

            reader.close();
            writer.close();

            checkBatchCompletion(batchId);

        } catch (Exception e) {
            log.error("-----------File processing failed: {}", inputPath, e);
            checkBatchCompletion(batchId);
        }
    }

    private void checkBatchCompletion(String batchId) {
        BatchStats stats = batchTracker.get(batchId);

        if (stats != null) {
            int remaining = stats.remainingFiles.decrementAndGet();

            if (remaining == 0) {
                long time = System.currentTimeMillis() - stats.startTime;

                log.info("------------BATCH COMPLETED | ID: {} | Time: {} ms", batchId, time);

                batchTracker.remove(batchId);
            }
        }
    }

    // =========================
    // HEADER PARSE (HDFS FIX)
    // =========================
    private HeaderInfo parseHeaderEfficiently(FileSystem fs, Path path) {

        HeaderInfo info = new HeaderInfo();
        info.branchCode = "00000";
        info.reportDate = "01012026";

        Pattern branchPattern = Pattern.compile("(?i)BRANCH\\s*::\\s*(\\d+)");
        Pattern datePattern = Pattern.compile("DATE\\s*::\\s*(\\S+)");

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(path)))) {

            String line;
            int count = 0;

            while ((line = reader.readLine()) != null && count < 50) {
                count++;

                Matcher m1 = branchPattern.matcher(line);
                if (m1.find()) {
                    info.branchCode = m1.group(1);
                }

                Matcher m2 = datePattern.matcher(line);
                if (m2.find()) {
                    info.reportDate = normalizeDate(m2.group(1));
                }
            }

        } catch (Exception e) {
            log.warn("Header parsing error", e);
        }

        return info;
    }

    private String normalizeDate(String dateStr) {
        if (dateStr == null) return "01012026";

        List<DateTimeFormatter> formats = Arrays.asList(
                DateTimeFormatter.ofPattern("dd-MM-yyyy"),
                DateTimeFormatter.ofPattern("dd/MM/yyyy"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd")
        );

        for (DateTimeFormatter f : formats) {
            try {
                return LocalDate.parse(dateStr, f)
                        .format(DateTimeFormatter.ofPattern("ddMMyyyy"));
            } catch (DateTimeParseException ignored) {}
        }

        return "01012026";
    }

    // =========================
    // CORE LOGIC (UNCHANGED)
    // =========================
    private String transformLine(String line, AsciiConfig config) {
        String[] cols = line.split("\\|", -1);

        log.info("--------------LINE: {}",line);
        log.info("---------------VALID: {}",isValidRow(cols,config));
        
        if (!isValidRow(cols, config)) return null;

        List<ProcessingItem> items = extractAmountColumns(cols, config);

        StringBuilder sb = new StringBuilder();
        boolean valid = false;

        for (ProcessingItem item : items) {
            BigDecimal amt = item.amount;

            if (amt.compareTo(BigDecimal.ZERO) != 0) {
                sb.append(amt.toPlainString());
                valid = true;
            }
        }

        if (valid) {
            return items.get(0).head + sb.toString();
        }

        return null;
    }

    private List<ProcessingItem> extractAmountColumns(String[] cols, AsciiConfig config) {
        List<ProcessingItem> list = new ArrayList<>();

        int idx = config.getInputHeadCol() - 1;
        String head = (idx < cols.length) ? cols[idx] : "";

        int col = Integer.parseInt(config.getAmountColSeq()) - 1;

        list.add(new ProcessingItem(head, parseAmount(cols, col), col + 1));

        return list;
    }

    private BigDecimal parseAmount(String[] cols, int idx) {
        if (idx >= cols.length) return BigDecimal.ZERO;
        return parseAmount(cols[idx]);
    }

    private BigDecimal parseAmount(String val) {
        try {
            if (val == null || val.trim().isEmpty()) return BigDecimal.ZERO;
            return new BigDecimal(val.trim().replace(",", ""));
        } catch (Exception e) {
            return BigDecimal.ZERO;
        }
    }

    private boolean isValidRow(String[] cols, AsciiConfig config) {
        if (cols.length < 2) return false;

        int idx = config.getInputHeadCol() - 1;
        if (cols.length <= idx) return false;

        String head = cols[idx];

        Pattern p = INPUT_REGEX_CACHE.computeIfAbsent(
                config.getInputHeadRegex(), Pattern::compile);

        return p.matcher(head).lookingAt();
    }

    private static class ProcessingItem {
        String head;
        BigDecimal amount;
        int colIndex;

        ProcessingItem(String h, BigDecimal a, int c) {
            head = h;
            amount = a;
            colIndex = c;
        }
    }

    private static class HeaderInfo {
        String branchCode;
        String reportDate;
    }
}

