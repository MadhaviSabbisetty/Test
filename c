import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.web.client.RestClientException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;





public Resource downloadAttachments(
        AttachmentDownloadRequest request) {

    File tempZip = null;

    try {

        log.info("Calling Report Service : {}",
                reportServiceUrl +
                        "/api/reports/attachment/download");

        tempZip = File.createTempFile(
                "attachments_",
                ".zip");

        File finalTempZip = tempZip;

        restClient.post()
                .uri(reportServiceUrl +
                        "/api/reports/attachment/download")
                .header(
                        "X-Internal-Token",
                        reportServiceInternalToken)
                .body(request)
                .exchange((clientRequest, clientResponse) -> {

                    if (!clientResponse.getStatusCode()
                            .is2xxSuccessful()) {

                        throw new RestClientException(
                                "Report Service returned HTTP status: "
                                        + clientResponse.getStatusCode());
                    }

                    try (InputStream inputStream =
                                 clientResponse.getBody()) {

                        java.nio.file.Files.copy(
                                inputStream,
                                finalTempZip.toPath(),
                                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    }

                    return null;
                });

        log.info(
                "Attachment ZIP downloaded successfully: {}",
                tempZip.getAbsolutePath());

        return new FileSystemResource(tempZip);

    } catch (Exception ex) {

        if (tempZip != null && tempZip.exists()) {
            boolean deleted = tempZip.delete();

            log.info(
                    "Deleted temporary ZIP after failure: {}",
                    deleted);
        }

        log.error(
                "Error while downloading attachments from Report Service",
                ex);

        throw new RuntimeException(
                "Unable to download attachments from Report Service",
                ex);
    }
}
