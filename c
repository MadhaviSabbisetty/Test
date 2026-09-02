@Override
public ReportStreamResponse downloadAttachments(
        AttachmentDownloadRequest request) throws IOException {

    log.info("Inside the report service attachment download method");

    if (request == null
            || request.getAttachments() == null
            || request.getAttachments().size() != 3) {

        throw new IllegalArgumentException(
                "Exactly 3 attachments are required");
    }

    Set<String> requiredFileNames = Set.of(
            "intr_report",
            "balance_compare_report",
            "cntr_report"
    );

    Set<String> actualFileNames = new HashSet<>();

    for (AttachmentRef attachment : request.getAttachments()) {

        if (attachment == null
                || attachment.getFileName() == null) {

            throw new IllegalArgumentException(
                    "Attachment filename is required");
        }

        actualFileNames.add(attachment.getFileName());
    }

    if (actualFileNames.size() != 3
            || !actualFileNames.equals(requiredFileNames)) {

        throw new IllegalArgumentException(
                "Attachments must contain exactly these filenames: " +
                "intr_report, balance_compare_report, cntr_report");
    }

    List<Path> attachmentPaths = new ArrayList<>();

    for (AttachmentRef attachment : request.getAttachments()) {

        String attachmentId = attachment.getAttachmentId();

        log.info("Attachment Id: {}", attachmentId);
        log.info("Report Name: {}", attachment.getFileName());
        log.info("Attachment Type: {}", attachment.getAttachmentType());

        if (attachmentId == null
                || !ATTACHMENT_PATTERN.matcher(attachmentId).matches()) {

            throw new IllegalArgumentException(
                    "Invalid attachment Id");
        }

        String date = attachmentId.substring(
                attachmentId.lastIndexOf("_") + 1);

        if (!date.matches("\\d{8}")) {
            throw new IllegalArgumentException(
                    "Invalid report date");
        }

        String folderDate =
                date.substring(4, 8) + "-"
                + date.substring(2, 4) + "-"
                + date.substring(0, 2);

        String actualFileName = attachmentId + ".xlsx";

        if (!ATTACHMENT_PATTERN.matcher(actualFileName).matches()) {
            throw new SecurityException(
                    "Invalid file name");
        }

        if (reportsBasePath.contains("..")) {
            throw new SecurityException(
                    "Invalid base path");
        }

        Path base = new Path(reportsBasePath);
        Path datePath = new Path(base, folderDate);
        Path filePath = new Path(datePath, actualFileName);

        String finalPath = filePath.toString();

        if (finalPath.contains("..")) {
            throw new SecurityException(
                    "Directory traversal detected");
        }

        Path normalizedPath =
                Path.getPathWithoutSchemeAndAuthority(filePath);

        if (!normalizedPath.equals(filePath)) {
            throw new SecurityException(
                    "Invalid HDFS path");
        }

        log.info("Checking HDFS file: {}", normalizedPath);

        if (!hdfsFileSystem.exists(normalizedPath)) {

            log.info("Attachment not found: {}", normalizedPath);

            throw new ResourceNotFoundException(
                    "Attachment not found: " + normalizedPath);
        }

        attachmentPaths.add(normalizedPath);
    }

    StreamingResponseBody streamingBody = outputStream -> {

        try (ZipOutputStream zipOut =
                     new ZipOutputStream(outputStream)) {

            byte[] buffer = new byte[BUFFER_SIZE];

            for (Path filePath : attachmentPaths) {

                String fileName = filePath.getName();

                log.info(
                        "Streaming attachment into ZIP: {}",
                        fileName);

                ZipEntry zipEntry =
                        new ZipEntry(fileName);

                zipOut.putNextEntry(zipEntry);

                try (FSDataInputStream inputStream =
                             hdfsFileSystem.open(filePath)) {

                    int bytesRead;

                    while ((bytesRead =
                            inputStream.read(buffer)) != -1) {

                        zipOut.write(
                                buffer,
                                0,
                                bytesRead);
                    }

                } finally {
                    zipOut.closeEntry();
                }

                log.info(
                        "Attachment streamed successfully: {}",
                        fileName);
            }

            zipOut.finish();

            log.info(
                    "All 3 attachments streamed successfully");
        }
    };

    return new ReportStreamResponse(
            "attachments.zip",
            streamingBody);
}
