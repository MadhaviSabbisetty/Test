finally {

    if (attachment != null) {

        try {

            attachment.getInputStream().close();

            if (attachment instanceof org.springframework.core.io.FileSystemResource) {

                java.io.File attachmentFile =
                        ((org.springframework.core.io.FileSystemResource) attachment)
                                .getFile();

                if (attachmentFile.exists()) {

                    boolean deleted =
                            attachmentFile.delete();

                    log.info(
                            "Deleted temporary attachment ZIP: {} Result: {}",
                            attachmentFile.getName(),
                            deleted);
                }
            }

            log.debug(
                    "Successfully purged attachment resources for Event: {}",
                    eventId);

        } catch (IOException e) {

            log.error(
                    "Failed to close/delete attachment resource for Event: {}",
                    eventId,
                    e);
        }
    }
}
