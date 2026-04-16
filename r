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
// import java.nio.file.DirectoryStream;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.nio.file.Paths;
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

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;


@Service
@Slf4j
public class AsciiGenerationService {

    @Autowired
    private HdfsService hdfsService;

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

            // Path baseDir = Paths.get(baseDirectoryPath);
            FileSystem fs=hdfsService.getFs();
            Path baseDir=new Path("/reports/");

            // if (!Files.exists(baseDir) || !Files.isDirectory(baseDir)) {
            //     throw new RuntimeException("Base directory does not exist: " + baseDirectoryPath);
            // }
            RemoteIterator<LocatedFileStatus> files=fs.listFiles(baseDir,true);

            List<Path> foundFiles = new ArrayList<>();
            // try (DirectoryStream<Path> stream = Files.newDirectoryStream(baseDir, entry ->
            //         Files.isRegularFile(entry) && entry.getFileName().toString().contains(searchKeyword))) {
            //     for (Path entry : stream) foundFiles.add(entry);
            // }
            while(files.hasNext()){
                LocatedFileStatus file=files.next();
                String fileName=file.getPath().getName();
                if(fileName.contains(searchKeyword) && fileName.endWith(".psv")){
                    foundFiles.add(file.getPath());
                }
            }

            if (foundFiles.isEmpty()) return "No files found for keyword: " + searchKeyword;

            String batchId = UUID.randomUUID().toString();
            batchTracker.put(batchId, new BatchStats(foundFiles.size()));

            log.info("Batch Started. Batch ID: {} | Total Files: {}", batchId, foundFiles.size());

            for (Path file : foundFiles) {
                jobQueueManager.submitFileJob(new FileJob(configId, file, batchId));
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
            FileSystem fs=hdfsService.getFs();
            AsciiConfig config = configRepo.findById(id).orElseThrow();

            // --- EDGE CASE FIX: Safe File Name Handling ---
            // String originalName = inputPath.getFileName().toString();
            // int dotIndex = originalName.lastIndexOf('.');
            // String baseName = (dotIndex == -1) ? originalName : originalName.substring(0, dotIndex);
            Path parent=inputPath.getParent();
            Path grandParent=parent.getParent();
            Path greatGrandParent=grandParent.getParent();

            String filename=grandParent.getName();
            String date=greatGrandParent.getName();

            String outputDirStr="/reports/"+date+"/"+filename+"_ascii/";
            Path outputDir =new Path(outputDirStr);

            if(!fs.exists(outputDir)){
                fs.mkdirs(outputDir);
            }

            String outputFileName = "GENERATED_" + config.getReportId() + "_" + System.currentTimeMillis() + config.getFileType();
            Path outputPath = new Path(outputDir,outputFileName);

            BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(inputPath)));
            BufferedWriter(new OutputStreamWriter(fs.create(outputPath)));

            // --- STREAMING PROCESSING ---
            // try (Stream<String> lines = Files.lines(inputPath);
            //      BufferedWriter writer = Files.newBufferedWriter(outputPath)) {

                // Header
                HeaderInfo headerInfo = parseHeaderEfficiently(inputPath);
                String branchPadded = String.format("%5s", headerInfo.branchCode).replace(' ', '0');
                String header = config.getOutputFirstLine() + branchPadded + headerInfo.reportDate + "F";

                writer.write(header);
                writer.newLine();

                List<String> lineBuffer = new ArrayList<>();
                int batchSize = config.getOutputPerLineHead();

                String line;
 
               while((line=reader.readLine())!=null) {
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
                }

                if (!lineBuffer.isEmpty()) {
                    writer.write(String.join("", lineBuffer));
                    writer.newLine();
                }

                if (config.getOutputEndLine() != null && !config.getOutputEndLine().isEmpty()) {
                    writer.write(config.getOutputEndLine());
                }
            reader.close();
            writer.close();

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



    mvn clean install
[INFO] Scanning for projects...
[INFO] 
[INFO] ---------------< com.tcs.fincore:AsciiGenerationService >---------------
[INFO] Building AsciiGenerationService 0.0.1-SNAPSHOT
[INFO]   from pom.xml
[INFO] --------------------------------[ jar ]---------------------------------
[WARNING] 2 problems were encountered while building the effective model for org.apache.yetus:audience-annotations:jar:0.5.0 during dependency collection step for project (use -X to see details)
[INFO] 
[INFO] --- clean:3.3.2:clean (default-clean) @ AsciiGenerationService ---
[INFO] 
[INFO] --- resources:3.3.1:resources (default-resources) @ AsciiGenerationService ---
[INFO] Copying 1 resource from src\main\resources to target\classes
[INFO] Copying 0 resource from src\main\resources to target\classes
[INFO]
[INFO] --- compiler:3.13.0:compile (default-compile) @ AsciiGenerationService ---
[INFO] Recompiling the module because of changed source code.
[INFO] Compiling 10 source files with javac [debug parameters release 21] to target\classes
[INFO] -------------------------------------------------------------
[ERROR] COMPILATION ERROR :
[INFO] -------------------------------------------------------------
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[95,64] cannot find symbol
  symbol:   method endWith(java.lang.String)
  location: variable fileName of type java.lang.String
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[151,13] cannot find symbol
  symbol:   method BufferedWriter(java.io.OutputStreamWriter)
  location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[162,17] cannot find symbol
  symbol:   variable writer
  location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[163,17] cannot find symbol
  symbol:   variable writer
  location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[176,33] cannot find symbol
  symbol:   variable writer
  location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[177,33] cannot find symbol
  symbol:   variable writer
  location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[182,63] cannot find symbol
  symbol:   method getFileName()
  location: variable inputPath of type org.apache.hadoop.fs.Path
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[187,21] cannot find symbol
  symbol:   variable writer
  location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[188,21] cannot find symbol
  symbol:   variable writer
  location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[192,21] cannot find symbol
  symbol:   variable writer
  location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[195,13] cannot find symbol
  symbol:   variable writer
  location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[314,38] cannot find symbol
  symbol:   variable Files
  location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[INFO] 12 errors
[INFO] -------------------------------------------------------------
[INFO] ------------------------------------------------------------------------
[INFO] BUILD FAILURE
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  4.988 s
[INFO] Finished at: 2026-04-16T18:25:59+05:30
[INFO] ------------------------------------------------------------------------
[ERROR] Failed to execute goal org.apache.maven.plugins:maven-compiler-plugin:3.13.0:compile (default-compile) on project AsciiGenerationService: Compilation failure: Compilation failure:
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[95,64] cannot find symbol
[ERROR]   symbol:   method endWith(java.lang.String)
[ERROR]   location: variable fileName of type java.lang.String
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[151,13] cannot find symbol
[ERROR]   symbol:   method BufferedWriter(java.io.OutputStreamWriter)
[ERROR]   location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[162,17] cannot find symbol
[ERROR]   symbol:   variable writer
[ERROR]   location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[163,17] cannot find symbol
[ERROR]   symbol:   variable writer
[ERROR]   location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[176,33] cannot find symbol
[ERROR]   symbol:   variable writer
[ERROR]   location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[177,33] cannot find symbol
[ERROR]   symbol:   variable writer
[ERROR]   location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[182,63] cannot find symbol
[ERROR]   symbol:   method getFileName()
[ERROR]   location: variable inputPath of type org.apache.hadoop.fs.Path
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[187,21] cannot find symbol
[ERROR]   symbol:   variable writer
[ERROR]   symbol:   variable writer
[ERROR]   location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR]   location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[188,21] cannot find symbol
[ERROR]   symbol:   variable writer
[ERROR]   location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[192,21] cannot find symbol
[ERROR]   symbol:   variable writer
[ERROR]   location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[195,13] cannot find symbol
[ERROR]   symbol:   variable writer
[ERROR]   location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] /D:/newWorkspace/fincore-1.2/Backend/AsciiGenerationService/src/main/java/com/tcs/fincore/AsciiGenerationService/Service/AsciiGenerationService.java:[314,38] cannot find symbol
[ERROR]   symbol:   variable Files
[ERROR]   location: class com.tcs.fincore.AsciiGenerationService.Service.AsciiGenerationService
[ERROR] -> [Help 1]
[ERROR]
[ERROR] To see the full stack trace of the errors, re-run Maven with the -e switch.
[ERROR] Re-run Maven using the -X switch to enable full debug logging.
[ERROR]
[ERROR] For more information about the errors and possible solutions, please read the following articles:
[ERROR] [Help 1] http://cwiki.apache.org/confluence/display/MAVEN/MojoFailureException

    
