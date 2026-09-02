@PostMapping("/attachment/download")
public ResponseEntity<StreamingResponseBody> downloadAttachments(
        @RequestBody AttachmentDownloadRequest request) {

    log.info("Received attachment download request");

    ReportStreamResponse response =
            reportService.downloadAttachments(request);

    return ResponseEntity.ok()
            .header(
                    HttpHeaders.CONTENT_DISPOSITION,
                    "attachment; filename=\"" +
                            response.getDownloadFileName() +
                            "\"")
            .contentType(MediaType.APPLICATION_OCTET_STREAM)
            .body(response.getStreamBody());
}
