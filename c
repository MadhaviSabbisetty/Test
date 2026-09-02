@Override
public Resource prepareAttachments(
        List<AttachmentRef> attachments)
        throws IOException {

    log.info("Preparing attachments");

    if (attachments == null || attachments.isEmpty()) {

        log.info("No attachments received.");

        return null;
    }

    if (attachments.size() != 3) {

        log.info("Exactly 3 attachments are required");

        throw new IllegalArgumentException(
                "Exactly 3 attachments are required");
    }

    Set<String> actualFileNames =
            new HashSet<>();

    for (AttachmentRef attachment : attachments) {

        if (attachment == null
                || attachment.getFileName() == null) {

            log.info("Attachment filename is required");

            throw new IllegalArgumentException(
                    "Attachment filename is required");
        }

        actualFileNames.add(
                attachment.getFileName());
    }

    if (actualFileNames.size() != 3
            || !actualFileNames.equals(REQUIRED_FILENAMES)) {

        log.info(
                "Attachments must contain exactly these filenames: " +
                "intr_report,balance_compare_report,cntr_report");

        throw new IllegalArgumentException(
                "Attachments must contain exactly these filenames: " +
                "intr_report,balance_compare_report,cntr_report");
    }

    AttachmentDownloadRequest request =
            AttachmentDownloadRequest.builder()
                    .attachments(attachments)
                    .build();

    Resource attachment =
            reportServiceClient.downloadAttachments(request);

    if (attachment == null
            || !attachment.exists()) {

        log.info("No attachments downloaded.");

        return null;
    }

    log.info(
            "Attachment ZIP prepared successfully: {}",
            attachment.getFilename());

    return attachment;
}
