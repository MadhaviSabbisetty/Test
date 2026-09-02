Resource attachment = null;

try {

    if (event.getAttachments() != null
            && !event.getAttachments().isEmpty()) {

        log.info(
                "Preparing {} attachment(s).",
                event.getAttachments().size());

        try {

            attachment =
                    attachmentPreparationService.prepareAttachments(
                            event.getAttachments());

            log.info("Attachments prepared successfully.");

        } catch (Exception e) {

            log.error(
                    "Attachment preparation failed",
                    e);

            throw new RuntimeException(
                    "Unable to prepare attachments",
                    e);
        }
    }
