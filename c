package com.fincore.CommunicationService.attachment;

import com.fincore.CommunicationService.client.ReportServiceClient;
import com.fincore.CommunicationService.dto.GenericCommunicationEvent.AttachmentRef;
import com.fincore.CommunicationService.dto.attachment.AttachmentDownloadRequest;
// import com.fincore.CommunicationService.dto.attachment.AttachmentDownloadResponse;
// import com.fincore.CommunicationService.dto.attachment.DownloadedAttachment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

// import java.io.File;
import java.io.IOException;
// import java.nio.file.Files;
// import java.util.ArrayList;
// import java.util.Base64;
import java.util.List;
// import java.util.Collections;

import java.util.Set;
import java.util.HashSet;
// import jakarta.validation.Valid;
// import jakarta.validation.constraints.NotNull;
// import jakarta.validation.constraints.Size;

import org.springfreamework.core.io.Resource;



@Slf4j
@Service
@RequiredArgsConstructor

public class AttachmentPreparationServiceImpl
        implements AttachmentPreparationService {

    private final ReportServiceClient reportServiceClient;

//     private final ZipService zipService;

//     @Value("${attachment.max.count:10}")
//     private int maxAttachmentCount;

//     @Value("${attachment.max.size.mb:20}")
//     private long maxAttachmentSizeMb;

    private static final Set<String> REQUIRED_FILENAMES= Set.of("intr_report","balance_compare_report","cntr_report");


//     @Override
//     public AttachmentDownloadResponse prepareAttachments(
//             List<AttachmentRef> attachments)
//             throws IOException {

//         log.info("Preparing attachments");

//         if (attachments == null || attachments.isEmpty()) {
//             log.info("No attachments received.");
//             return new AttachmentDownloadResponse();
//         }

//         //Exactly 3 attachments are required
//         if(attachments.size()!=3)
//         {                
//                 log.info("Exactly 3 attachments are required");
//                 throw new IllegalArgumentException("Exactly 3 attachments are required");
//         }

//         //Filename is required for attachments 
//         Set<String> actualFileNames= new HashSet<>();

//         for(AttachmentRef attachment : attachments){
//                 if(attachment == null || attachment.getFileName()== null){
//                         log.info("Attachment filename is required");
//                         throw new IllegalArgumentException("Attachment filename is required");
//                 }
//                 actualFileNames.add(attachment.getFileName());
//         }
        


//         if(actualFileNames.size()!=3 || !actualFileNames.equals(REQUIRED_FILENAMES)){
//                 log.info("Attachments must contain exactly these filenames:"+"intr_report,balance_compare_report,cntr_report");
//                 throw new IllegalArgumentException("Attachments must contain exactly these filenames:"+"intr_report,balance_compare_report,cntr_report");
//         }

        
//         AttachmentDownloadRequest request =
//                 AttachmentDownloadRequest.builder()
//                         .attachments(attachments)
//                         .build();


//         AttachmentDownloadResponse response =
//                 reportServiceClient.downloadAttachments(request);

//         if (response == null
//                 || response.getAttachments() == null
//                 || response.getAttachments().isEmpty()) {

//             log.info("No attachments downloaded.");
//             return new AttachmentDownloadResponse();
//         }

        
//         long totalSize = 0;

//         for (DownloadedAttachment attachment :
//                 response.getAttachments()) {

//             totalSize += attachment.getFileSize();
//         }

//         boolean zipRequired =
//                 response.getAttachments().size() > maxAttachmentCount
//                         ||
//                 totalSize > (maxAttachmentSizeMb * 1024 * 1024);

//         if (!zipRequired) {

//             log.info("ZIP not required.");
//             return response;
//         }

//         log.info("ZIP generation required.");

//         List<File> files = new ArrayList<>();

//         for (DownloadedAttachment attachment :
//                 response.getAttachments()) {

//             byte[] bytes =
//                     Base64.getDecoder().decode(
//                             attachment.getBase64());

//             File tempFile =
//                     File.createTempFile(
//                             "attachment_",
//                             "_" + attachment.getFileName());

//             Files.write(tempFile.toPath(), bytes);

//             files.add(tempFile);
//         }

//         File zipFile = zipService.createZip(files);

//         deleteTemporaryFiles(files);

//         byte[] zipBytes =
//                 Files.readAllBytes(zipFile.toPath());

//         zipFile.delete();

//         DownloadedAttachment zipAttachment =
//                 new DownloadedAttachment();

//         zipAttachment.setFileName("attachments.zip");

//         zipAttachment.setContentType("application/zip");

//         zipAttachment.setFileSize(zipBytes.length);

//         zipAttachment.setBase64(
//                 Base64.getEncoder()
//                         .encodeToString(zipBytes));

//         AttachmentDownloadResponse zipResponse =
//                 new AttachmentDownloadResponse();

//         zipResponse.setAttachments(
//                 Collections.singletonList(zipAttachment));

//         return zipResponse;
//     }




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

//     public void deleteTemporaryFiles(List<File> files) {

//         if (files == null || files.isEmpty()) {
//             return;
//         }

//         for (File file : files) {

//             try {

//                 if (file.exists()) {

//                     boolean deleted = file.delete();

//                     log.info("Deleted temp file : {} Result : {}",
//                             file.getName(),
//                             deleted);
//                 }

//             } catch (Exception ex) {

//                 log.error("Unable to delete temp file : {}",
//                         file.getAbsolutePath(),
//                         ex);
//             }
//         }
//     }
}
