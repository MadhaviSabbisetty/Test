Attachment Service to Report Service  : 

Objective
Download report attachments before Email dispatch.

Responsibilities
1. Create AttachmentService.
2. Receive attachment references from GenericCommunicationEvent.
3. Download reports from Report Service using Spring RestClient.

Attachment Flow

attachmentId
       ?
Report Service
       ?
Download Report
       ?
Communication Service

If multiple reports exist:
- Download all reports.
- Calculate attachment count and total size.
- If configured threshold is exceeded,
automatically create ZIP.
- Otherwise send individual attachments.
Delete temporary files after email is sent.



Use Spring RestClient.
Reason
- Simple synchronous internal service call.
- Better streaming support.
- Lower overhead.
- Better timeout handling.
- Recommended modern Spring client.
- No Eureka/OpenFeign required.
Do NOT
- Send reports through Kafka.
- Store reports permanently.
- Leave temporary files after processing.



zip logic :
==============================================================
1-5 PDFs? 
Individual attachments

6-10 PDFs ?
Still individual (if size permits)

20 PDFs ?
ZIP


core flow ; 
==============================================================
AttachmentPreparationService
prepareAttachments(List<AttachmentRef> refs)
?
Download all reports
?
Calculate
attachmentCount
totalSize
?
if (attachmentCount > MAX_ATTACHMENTS
   || totalSize > MAX_EMAIL_SIZE)
?
createZip()
?
return zip
else
?
return individual files
 
