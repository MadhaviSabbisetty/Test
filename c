report service
package com.fincore.ReportService.controller;

import com.fincore.ReportService.dto.AttachmentDownloadRequest;
import com.fincore.ReportService.dto.AttachmentDownloadResponse;
import com.fincore.ReportService.service.AttachmentService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/attachment")
public class AttachmentController{
    private final AttachmentService attachmentService;
    
    @PostMapping("/download")
    public AttachmentDownloadResponse downloadAttachment(@RequestBody AttachmentDownloadRequest request){
        return attachmentService.downloadAttachments(request);
        
    }
}
package com.fincore.ReportService.dto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor

public class AttachmentDownloadRequest{
    //private String filePath;
    private List<AttachmentReference> attachments;
}
package com.fincore.ReportService.dto;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AttachmentDownloadResponse{

    private String fileName;

    private String contentType;

    private String base64;

    private long fileSize;

    
}
package com.fincore.ReportService.dto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class AttachmentReference{

    private String reportName;

    private Long reportId;

    private String storageType;

    private String path;
}
package com.fincore.ReportService.service;
import com.fincore.ReportService.dto.AttachmentDownloadRequest;
import com.fincore.ReportService.dto.AttachmentDownloadResponse;

public interface AttachmentService{
    AttachmentDownloadResponse downloadAttachments(AttachmentDownloadRequest request);
    
}package com.fincore.ReportService.service;

import com.fincore.ReportService.dto.AttachmentDownloadRequest;
import com.fincore.ReportService.dto.AttachmentDownloadResponse;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;

@Service
@RequiredArgsConstructor
public class AttachmentServiceImpl implements AttachmentService {

    private final FileSystem hdfsFileSystem;

    @Override
    public AttachmentDownloadResponse downloadAttachments(
            AttachmentDownloadRequest request) {

        try {

            Path filePath = new Path(request.getFilePath());

            if (!hdfsFileSystem.exists(filePath)) {
                throw new RuntimeException(
                        "Attachment not found : " + request.getFilePath());
            }

            FileStatus status = hdfsFileSystem.getFileStatus(filePath);

            byte[] fileBytes;

            try (FSDataInputStream inputStream =
                         hdfsFileSystem.open(filePath);
                 ByteArrayOutputStream outputStream =
                         new ByteArrayOutputStream()) {

                byte[] buffer = new byte[8192];
                int bytesRead;

                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }

                fileBytes = outputStream.toByteArray();
            }

            AttachmentDownloadResponse response =
                    new AttachmentDownloadResponse();

            response.setFileName(filePath.getName());

            response.setContentType(getContentType(filePath.getName()));

            response.setFileSize(status.getLen());

            response.setBase64(
                    Base64.getEncoder().encodeToString(fileBytes));

            return response;

        } catch (IOException ex) {

            throw new RuntimeException(
                    "Unable to download attachment", ex);
        }
    }

    /**
     * Returns MIME type based on extension.
     */
    private String getContentType(String fileName) {

        String lower = fileName.toLowerCase();

        if (lower.endsWith(".pdf")) {
            return "application/pdf";
        }

        if (lower.endsWith(".xlsx")) {
            return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
        }

        if (lower.endsWith(".xls")) {
            return "application/vnd.ms-excel";
        }

        if (lower.endsWith(".csv")) {
            return "text/csv";
        }

        if (lower.endsWith(".psv")) {
            return "text/plain";
        }

        if (lower.endsWith(".zip")) {
            return "application/zip";
        }

        return "application/octet-stream";
    }
}
