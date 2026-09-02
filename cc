package com.fincore.ReportService.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fincore.ReportService.dto.*;
import com.fincore.commonutilities.jwt.JwtUtil;
import lombok.extern.slf4j.Slf4j;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fincore.ReportService.service.ReportService;


@Slf4j
@RestController
@RequestMapping("/api/reports")
public class ReportController {

        private final ReportService reportService;
        private final JwtUtil jwtUtil; // Declare an instance field

        public ReportController(ReportService reportService, JwtUtil jwtUtil) {
                this.reportService = reportService;
                this.jwtUtil = jwtUtil;
        }

        @PostMapping("/types")
        public List<ReportTypeDto> getReportTypes(@RequestHeader("Authorization") String token,
                        @RequestBody ReportTypesRequest request) {
                log.info("Received request to fetch all available report types for" +
                                "module: {} for role: {}", request.getModuleType(),
                                jwtUtil.getUserRoleFromToken(token));
                return reportService.getReportTypes(jwtUtil.getUserRoleFromToken(token), request.getModuleType());
        }

        @GetMapping("/adhoc-types")
        public List<ReportTypeDto> getAdhocReportTypes(@RequestHeader("Authorization") String token) {
                log.info("Received request to fetch all available report types for" +
                                "for role: {}", jwtUtil.getUserRoleFromToken(token));
                return reportService.getAdhocReportTypes(jwtUtil.getUserRoleFromToken(token));
        }




        @PostMapping("/download")
        public ResponseEntity<StreamingResponseBody> downloadReport(
                        @RequestHeader("Authorization") String token,
                        @RequestBody ReportDownloadRequest request)  {

                log.info("Received request to download report from HDFS for this criteria: {}", request);

                ReportStreamResponse response = reportService.downloadReportStream(
                                request.getFileName(),
                                request.getDate(),
                                jwtUtil.getUserRoleFromToken(token),
                                jwtUtil.getUserIdFromToken(token),
                                request.getBranchCode());

                return ResponseEntity.ok()
                                // This header tells the browser "This is a file download"
                                .header(HttpHeaders.CONTENT_DISPOSITION,
                                                "attachment; filename=\"" + response.getDownloadFileName() + "\"")
                                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                                .body(response.getStreamBody());
        }

        @PostMapping("/create-reports")
        public Map<String, String> createReports(@RequestHeader("Authorization") String token,
                        @RequestBody ReportCreationDto requestBody) throws IOException {
                // EXTRACT USER ID FROM TOKEN
                String userId = jwtUtil.getUserIdFromToken(token);
                return reportService.createReports(requestBody, userId);
        }

        @PostMapping("/generate-reports")
        public ReportGenerationResponseDTO generateReports(@RequestBody ReportGenerationDTO requestBody)
                        throws IOException {

                return reportService.generateReports(requestBody);
        }

        @PostMapping("/download-temp-reports")
        public ResponseEntity<StreamingResponseBody> downloadTemporaryReports(
                        @RequestHeader("Authorization") String token,
                        @RequestBody ReportDownloadRequest request) throws IOException {

                log.info("Received request to download temp report from HDFS for this criteria: {}", request);

                ReportStreamResponse response = reportService.downloadTempReportStream(
                                request.getFileName(),
                                request.getDate(),
                                jwtUtil.getUserRoleFromToken(token),
                                jwtUtil.getUserIdFromToken(token),
                                request.getRunId());

                return ResponseEntity.ok()
                                // This header tells the browser "This is a file download"
                                .header(HttpHeaders.CONTENT_DISPOSITION,
                                                "attachment; filename=\"" + response.getDownloadFileName() + "\"")
                                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                                .body(response.getStreamBody());
        }

        @PostMapping("/generate/base64")
        public ResponseEntity<ReportBase64ResponseDTO> generateReportBase64(
                        @RequestBody ReportBase64RequestDTO request) throws IOException {

                ReportBase64ResponseDTO response = reportService.generateReportBase64(request);

                return ResponseEntity.ok(response);
        }

         /**
         * Exposes attachment download endpoint for Communication Service
         * Downloads requested report attachments from HDFS
        */

        @PostMapping("/attachment/download")
        public ResponseEntity<ApiResponse<AttachmentDownloadResponse>> downloadAttachments(@RequestBody AttachmentDownloadRequest request){
                              
                               try{
                                        log.info("inside report controller attachment endpoint");
                                        AttachmentDownloadResponse response=reportService.downloadAttachments(request); 
                                        return ResponseEntity.ok(ApiResponse.success(response,"Attachment downloaded successfully"));
                               }
                               catch(IOException e){
                                        log.error("Attachment download failed",e); 
                                        return ResponseEntity.ok(
                                                ApiResponse.error(e.getMessage() == null ?"Attachment download failed.":e.getMessage())
                                );
                               }

        }
}
package com.fincore.ReportService.service;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import com.fincore.ReportService.dto.*;

public interface ReportService {

        List<ReportTypeDto> getReportTypes(int roleId, String moduleType);


        List<ReportTypeDto> getAdhocReportTypes(int roleId);

        ReportStreamResponse downloadReportStream(String fileName, LocalDate date, int userRoleId, String userId,
                        String branchCode) ;

        Map<String, String> createReports(ReportCreationDto parameters, String userId) throws IOException;

     
        ReportStreamResponse downloadTempReportStream(String fileName, LocalDate date, int userRoleId, String userId,
                        String runId) throws IOException;

  
        ReportGenerationResponseDTO generateReports(ReportGenerationDTO parameters) throws IOException;

        ReportBase64ResponseDTO generateReportBase64(ReportBase64RequestDTO request) throws IOException;


        AttachmentDownloadResponse downloadAttachments(AttachmentDownloadRequest request) throws IOException;

}
