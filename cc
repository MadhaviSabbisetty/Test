 const handleSubmitBatchCancel = async () => {
    setIsSubmitting(true);
    const targetBatchId = selectedBatch.batchId; // Store it before modal closes

    try {
      await callApi(
        `/JS/voucher/my-requests/by-batch/${targetBatchId}`,
        null,
        "DELETE"
      );
      showSnackBar(
        `Batch ${targetBatchId} deletion process started.`,
        "success"
      );

      setRows((prevRows) =>
        prevRows.filter((row) => row.batchId !== targetBatchId)
      );

      handleCloseModal();

      setTimeout(() => {
        fetchMyBatches();
      }, 1500);
    } catch (error) {
      const serverMsg = error?.response?.data?.message
        || error?.response?.data?.error
        || (typeof error?.response?.data === 'string' ? error.response.data : null)
        || error?.message;

      showSnackBar(serverMsg || "Failed to delete batch. It may have already been processed.", "error");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleOpenViewModal = async (row) => {
    // 1. CLEAN SLATE FIX: Wipe all old data BEFORE opening the modal so nothing flashes!
    setVidDetails(null);
    setDynamicSteps([]);
    setWorkflowHistory([]);
    setIsDescExpanded(false);
    setViewingBatchId(row.batchId);
    setViewingBatchCreatorId(row.creatorId);
    setViewingBatch(row);
    setViewingBatchState(row.currentStatus || "");
    setViewingBatchRawStatus(row.batchStatus || "");
    setViewingBatchRequestDate(row.requestDate);
    setViewingBatchRequestCount(row.requestCount || 0);
    setViewingBatchOrder(row.currentOrder);
    setViewingBatchTimeSinceLastAction(row.timeSinceLastAction || "");

    setIsViewModalOpen(true); // Open modal immediately
    setIsFetchingSteps(true);
    setIsFetchingVid(true);

    try {
      const workflowCode = row.approvalWorkflow || "Workflow_3";
      const [stepsData, summaryData, historyData] = await Promise.all([
        callApi(`/JS/voucher/workflow/steps/${workflowCode}`, null, "GET"),
        callApi(`/JS/voucher/batch-summary/${row.batchId}`, null, "GET"),
        callApi(`/JS/voucher/workflow-history/${row.batchId}`, null, "GET"),
      ]);

      setDynamicSteps(Array.isArray(stepsData) ? stepsData : []);
      setVidDetails(summaryData);
      setWorkflowHistory(Array.isArray(historyData) ? historyData : []);
    } catch (err) {
      showSnackBar("Failed to load details.", "error");
    } finally {
      setIsFetchingSteps(false);
      setIsFetchingVid(false);
    }
  };

  const handleCloseViewModal = () => {
    if (document.activeElement instanceof HTMLElement) {
      document.activeElement.blur();
    }
    document.body.focus();

    // 2. SMOOTH CLOSE FIX: Set the modal to false IMMEDIATELY to trigger the MUI fade-out animation.
    setIsViewModalOpen(false);

    // 3. Wait for the 300ms fade-out animation to finish, THEN wipe the memory.
    setTimeout(() => {
      setViewingBatchId(null);
      setViewingBatch(null);
      setIsDescExpanded(false);
      setVidDetails(null);
      setDynamicSteps([]);
      setWorkflowHistory([]);
    }, 300);
  };


  const handleDetailPageChange = (event, newPage) => {
    setDetailPage(newPage);
    fetchDetailRows(viewingBatchId, newPage, detailRowsPerPage);
  };
  const handleDetailRowsPerPageChange = (event) => {
    const newSize = parseInt(event.target.value, 10);
    setDetailRowsPerPage(newSize);
    setDetailPage(0);
    fetchDetailRows(viewingBatchId, 0, newSize);
  };

  const handleCancelSelectedJournals = async () => {
    if (selectedJournalPrefixes.length === 0) {
      showSnackBar("Select at least one entry.", "warning");
      return;
    }
    setIsPartialSubmitting(true);
    try {
      await callApi(
        "/JS/voucher/my-requests/by-journal-list",
        selectedJournalPrefixes,
        "DELETE"
      );
      showSnackBar("Selected entries deleted successfully.", "success");
      handleCloseViewModal();
    } catch (error) {
      showSnackBar("Failed to delete selection", "error");
      setIsPartialSubmitting(false);
    }
  };

  // Standardized normalization for matching user contexts safely
  const isViewingSelfCreated =
    currentUserId &&
    viewingBatchCreatorId &&
    String(currentUserId).trim().toUpperCase() === String(viewingBatchCreatorId).trim().toUpperCase();

  // batch delete
  const isBatchDeletable =
    viewingBatchRawStatus === "P" &&
    Number(viewingBatchOrder) === 2 &&
    !isFetchingSteps &&
    workflowHistory.length <= 1;

  const columns = [
    {
      field: "batchId",
      headerName: "Batch ID",
      width: 120,
      align: "center",
      headerAlign: "center",
      disableColumnMenu: false, // <--- ENABLED
      filterable: true,         // <--- ENABLED
      sortable: false,
    },
    {
      field: "vid",
      headerName: "VID",
      width: 150,
      align: "center",
      headerAlign: "center",
      disableColumnMenu: false, // <--- ENABLED
      filterable: true,         // <--- ENABLED
      sortable: false,
    },
    {
      field: "currentStatus",
      headerName: "Workflow Progress",
      width: 440,
      align: "center",
      headerAlign: "center",
      disableColumnMenu: true,
      sortable: false,
      renderCell: (params) => {
        if (params.row.batchStatus === 'S') {
          return (
            <Box sx={{ width: "100%", display: "flex", alignItems: "center", justifyContent: "flex-start", pl: 2, height: "100%" }}>
              <Chip icon={<CircularProgress size={16} sx={{ color: 'primary.main' }} />} label="Creating Batch..." variant="outlined" color="primary" sx={{ fontWeight: 'bold', borderStyle: 'dashed' }} />
            </Box>
          );
        }

        const state = params.value || "";
        const workflow = params.row.approvalWorkflow;
        const isRejected = params.row.currentStatus === "REJECTED" || params.row.batchStatus === "R";
        const steps = workflowStepsMap[workflow] || [];

        let activeStep = params.row.currentOrder ? params.row.currentOrder - 1 : 0;

        // PERFECTED STEPPER LOGIC:
        if (params.row.isExecutionFailed) {
          activeStep = steps.length - 1;
        } else if (!isRejected && (state === "POSTED" || state === "C" || params.row.currentStatus === "POSTED")) {
          // 1. ONLY push past the end (All Green) if it is fully completed and posted
          activeStep = steps.length;
        } else if (!isRejected && (state === "SCHEDULED" || state === "WAITING FOR EXECUTION" || state === "APPROVED")) {
          // 2. If it is waiting for the scheduler, PAUSE on the final Execution step!
          activeStep = steps.length - 1;
        }

        if (activeStep === -1) activeStep = 0;


        const escalationHours = params.row.escalationTime || 24;
        const hoursPending = Math.abs(new Date() - new Date(params.row.requestDate)) / 36e5;
        const isEscalated = !isRejected && activeStep < steps.length && hoursPending > escalationHours;

        return (
          <Box sx={{ width: "100%", display: "flex", alignItems: "center", justifyContent: "flex-start", pl: 2, height: "100%" }}>
            <Stepper activeStep={activeStep} alternativeLabel connector={<CustomConnector />} sx={{ width: "100%", mt: 0.5 }}>
              {steps.map((step, index) => {
                const isCompleted = index < activeStep;
                const isCurrent = index === activeStep;

                let statusFlag = "PENDING";
                if (isCompleted) statusFlag = "COMPLETED";
                if (isCurrent && isRejected) statusFlag = "REJECTED";
                // NEW FIX: Turn the Execution stage RED with an X if it failed!
                else if (isCurrent && params.row.isExecutionFailed) statusFlag = "REJECTED";
                else if (isCurrent && isEscalated) statusFlag = "ESCALATED";
                else if (isCurrent) statusFlag = "ACTIVE";

                const label = step?.designation || step?.state || "Unknown";

                return (
                  <Tooltip title={statusFlag === "ESCALATED" ? "Escalation Time lapsed - Attention Required" : label} key={`${label}-${index}`} arrow>
                    <Step completed={isCompleted} active={isCurrent} sx={{ px: 0, opacity: (isRejected || params.row.isExecutionFailed) && index > activeStep ? 0.3 : 1 }}>
                      <StepLabel
                        StepIconComponent={() => (
                          <CustomWorkflowIcon statusFlag={statusFlag} iconLetter={label.charAt(0).toUpperCase()} label={label} isMini={true} />
                        )}
                        sx={{ p: 0, m: 0 }}
                      />
                    </Step>
                  </Tooltip>
                );
              })}
            </Stepper>
          </Box>
        );
      },
    },
    {
      field: "creatorId",
      headerName: "Creator",
      width: 300,
      align: "center",
      headerAlign: "center",
      disableColumnMenu: true,
      sortable: false,

      cellClassName: "creator-cell",

      renderCell: (p) => {
        const name = p.row.creatorName || p.value;
        const id = p.value;

        return (
          <Stack
            direction="row"
            alignItems="center"
            justifyContent="center"
            spacing={1}
            sx={{
              width: "100%",
              height: "100%",
              overflow: "visible !important",
              px: 1,
            }}
          >
            <AccountCircleIcon sx={{ color: "text.secondary", fontSize: 20 }} />
            <Typography
              variant="body2"
              sx={{
                whiteSpace: "nowrap",
                overflow: "visible !important",
                textOverflow: "clip",
                textAlign: "center",
              }}
            >
              {name} <strong>({id})</strong>
            </Typography>
          </Stack>
        );
      },
    },
    {
      field: "requestDate",
      headerName: "Submitted On",
      width: 200,
      align: "center",
      headerAlign: "center",
      disableColumnMenu: true,
      sortable: false,
      renderCell: (p) => formatDateTime(p.value),
    },

    {
      field: "requestCount",
      headerName: "No. of Entries",
      width: 150,
      align: "center",
      headerAlign: "center",
      disableColumnMenu: true,
      sortable: false,
    },
    {
      field: "actions",
      headerName: "Action",
      width: 140,
      align: "center",
      headerAlign: "center",
      disableColumnMenu: true,
      sortable: false,
      renderCell: (params) => {
        const isDraft = params.row.batchStatus === 'S'; // NEW: Check if still compiling

        return (
          <Tooltip title={isDraft ? "Data is currently being compiled. Please wait." : "View & Actions"} arrow>
            <span> {/* Required span to allow Tooltip to show over a disabled button */}
              <Button
                variant="contained"
                size="small"
                disabled={isDraft} // NEW: Locks the button
                onClick={() => handleOpenViewModal(params.row)}
                sx={{ textTransform: "none", borderRadius: 2, boxShadow: 0 }}
              >
                View & Action
              </Button>
            </span>
          </Tooltip>
        );
      },
    },
  ];

  return (
    <Paper elevation={0}>
      <Typography variant="h6" fontWeight="bold" gutterBottom color="primary.main" sx={{ ml: 6, mt: 2 }}>
        Voucher Posting Status
      </Typography>
      <Box sx={styles.mainContainer}>


        <DataGrid
          rows={rows}
          columns={columns}
          loading={loading}
          getRowId={(row) => row.batchId}
          disableRowSelectionOnClick
          pageSizeOptions={[5, 10, 25, 50]} // <--- Added 50 here
          disableColumnResize
          rowHeight={52}
          // --- NEW: SERVER SIDE PAGINATION PROPS ---
          paginationMode="server"
          rowCount={totalRowCount}
          paginationModel={paginationModel}
          onPaginationModelChange={setPaginationModel}
          // -----------------------------------------

          filterMode="server"
          filterModel={filterModel}
          onFilterModelChange={setFilterModel}

          slots={{
            noRowsOverlay: CustomNoRowsOverlay,
            toolbar: GridToolbar
          }}


          sx={styles.dataGridContainer}
        />
        <Dialog
          open={isModalOpen}
          onClose={handleCloseModal}
          fullWidth
          maxWidth="sm"
        >
          <DialogTitle sx={{ display: "flex", alignItems: "center" }}>
            <DeleteSweepIcon sx={{ mr: 1, color: "error.main" }} /> Confirm Deletion
          </DialogTitle>
          <DialogContent>
            <DialogContentText>
              Are you sure you want to delete batch <strong>{selectedBatch?.batchId}</strong>? This item will be permanently deleted
            </DialogContentText>
          </DialogContent>
          <DialogActions sx={{ p: 3, borderTop: "1px solid", borderColor: "divider" }}>
            <Button onClick={handleCloseModal} disabled={isSubmitting}>Cancel</Button>
            <Button
              variant="contained"
              color="error"
              startIcon={isSubmitting ? <CircularProgress size={20} color="inherit" /> : <DeleteIcon />}
              onClick={handleSubmitBatchCancel}
              disabled={isSubmitting}
            >
              {isSubmitting ? "Deleting..." : "Confirm Delete"}
            </Button>
          </DialogActions>
        </Dialog>


        {/* --- WORKFLOW DETAILS MODAL --- */}
        <Dialog
          open={isViewModalOpen}
          onClose={() => {
            handleBlurAll(); // <--- KILL FOCUS WHEN CLICKING BACKDROP
            handleCloseViewModal();
          }}
          fullWidth
          maxWidth="lg"
        >
          <DialogTitle sx={styles.dialogTitle}>
            <Typography variant="h6" fontWeight="bold">
              Batch Workflow Details: {viewingBatchId}
            </Typography>
            <IconButton onClick={handleCloseViewModal}>
              <CloseIcon />
            </IconButton>
          </DialogTitle>

          <DialogContent dividers sx={{ p: 0, backgroundColor: "#f8fafc", height: '75vh', display: 'flex', flexDirection: 'column' }}>
            <Box sx={{ display: "flex", flexDirection: "column", flexGrow: 1, overflowY: "auto" }}>

              {/* ---  HEADER CARDS --- */}
              <Box sx={{ p: 4, backgroundColor: "white", borderBottom: "1px solid", borderColor: "divider" }}>
                {isFetchingVid ? (
                  /* LAYOUT SHIFT FIX: Added minHeight: 300px so it holds space for the incoming cards */
                  <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '300px', p: 4 }}>
                    <CircularProgress size={32} />
                  </Box>
                ) : vidDetails && vidDetails.VID ? (
                  <Stack spacing={3} sx={{ width: '100%' }}>
                    {/* SECTION 1: VOUCHER DETAILS */}
                    <Paper elevation={0} sx={{ p: 3, borderRadius: 2, width: '100%', border: "1px solid", borderColor: "divider", display: 'flex', flexDirection: 'column' }}>
                      <Typography variant="subtitle1" fontWeight="bold" color="text.primary" gutterBottom sx={{ borderBottom: "1px solid", borderColor: "divider", pb: 1, mb: 2 }}>
                        Voucher Request ID : {vidDetails.VID}
                      </Typography>
                      <Typography variant="caption" color="text.secondary" display="block" gutterBottom>Voucher Description</Typography>
                      <Box sx={{ maxHeight: isDescExpanded ? 200 : 90, mb: "24px", overflowY: "auto", pr: 1, transition: "max-height 0.3s ease" }}>
                        <Typography variant="body2" color="text.primary" sx={{ whiteSpace: "pre-wrap", lineHeight: 1.6 }}>
                          {(vidDetails.DESCRIPTION || "").length > 100 && !isDescExpanded
                            ? `${vidDetails.DESCRIPTION.substring(0, 100)}...`
                            : (vidDetails.DESCRIPTION || "No description provided.")}

                          {(vidDetails.DESCRIPTION || "").length > 100 && (
                            <Typography
                              component="span"
                              color="primary"
                              onClick={() => setIsDescExpanded(!isDescExpanded)}
                              sx={{ cursor: "pointer", ml: 1, fontWeight: "bold", '&:hover': { textDecoration: 'underline' } }}
                            >
                              {isDescExpanded ? "Read Less" : "Read More"}
                            </Typography>
                          )}
                        </Typography>
                      </Box>
                      <Grid item xs={12} sm={3} sx={{ mb: "24px" }}>
                        <Typography variant="caption" color="text.secondary" display="block" gutterBottom>
                          Issue Categories
                        </Typography>
                        <Typography variant="body1" fontWeight="600" color="primary.main">
                          {vidDetails.ISSUE_CATEGORIES || "N/A"}
                        </Typography>
                      </Grid>
                      <Grid container spacing={3}>
                        <Grid item xs={12} sm={4}>
                          <Typography variant="caption" color="text.secondary" display="block" gutterBottom>Category</Typography>
                          <Typography variant="body1" fontWeight="500">{vidDetails.CATEGORY_NAME}</Typography>
                        </Grid>
                        <Grid item xs={12} sm={4}>
                          <Typography variant="caption" color="text.secondary" display="block" gutterBottom>Created By</Typography>
                          <Stack direction="row" spacing={1} alignItems="center">
                            <AccountCircleIcon sx={{ fontSize: 20, color: "text.secondary" }} />
                            <Typography variant="body1" fontWeight="500">
                              {vidDetails.VID_CREATED_BY_NAME} <Typography component="span" variant="caption" color="text.secondary">({vidDetails.VID_CREATOR_ID})</Typography>
                            </Typography>
                          </Stack>
                        </Grid>
                        <Grid item xs={12} sm={4}>
                          <Typography variant="caption" color="text.secondary" display="block" gutterBottom>Created At</Typography>
                          <Typography variant="body1" fontWeight="500">
                            {formatDateTime(vidDetails.VID_CREATED_AT)}
                          </Typography>
                        </Grid>
                      </Grid>
                    </Paper>

                    {/* SECTION 2: BATCH DETAILS */}
                    <Paper elevation={0} sx={{ p: 3, borderRadius: 2, width: '100%', border: "1px solid", borderColor: "divider", display: 'flex', flexDirection: 'column' }}>
                      <Typography variant="subtitle1" fontWeight="bold" color="text.primary" gutterBottom sx={{ borderBottom: "1px solid", borderColor: "divider", pb: 1, mb: 2 }}>
                        Voucher Details
                      </Typography>
                      <Grid container spacing={3} alignItems="center">
                        <Grid item xs={12} sm={3}>
                          <Typography variant="caption" color="text.secondary" display="block" gutterBottom>Created By</Typography>
                          <Stack direction="row" spacing={1} alignItems="center">
                            <AccountCircleIcon sx={{ fontSize: 20, color: "text.secondary" }} />
                            <Typography variant="body1" fontWeight="500">
                              {vidDetails.BATCH_CREATED_BY_NAME} <Typography component="span" variant="caption" color="text.secondary">({vidDetails.BATCH_CREATOR_ID})</Typography>
                            </Typography>
                          </Stack>
                        </Grid>
                        <Grid item xs={12} sm={2}>
                          <Typography variant="caption" color="text.secondary" display="block" gutterBottom>Created At</Typography>
                          <Typography variant="body1" fontWeight="500">
                            {formatDateTime(vidDetails.BATCH_CREATED_AT)}
                          </Typography>
                        </Grid>
                        <Grid item xs={12} sm={2}>
                          <Typography variant="caption" color="text.secondary" display="block" gutterBottom>No. of Entries</Typography>
                          <Typography variant="body1" fontWeight="600" color="primary.main">
                            {viewingBatchRequestCount}
                          </Typography>
                        </Grid>
                        <Grid item xs={12} sm={3}>
                          <Typography variant="caption" color="text.secondary" display="block" gutterBottom>Is Execution Scheduled?</Typography>
                          <Typography variant="body1" fontWeight="500" color={vidDetails.SCHEDULED_TIME ? "info.main" : "text.primary"}>
                            {vidDetails.SCHEDULED_TIME ? (
                              <>
                                Yes ({formatDateTime(vidDetails.SCHEDULED_TIME)})
                                {new Date(vidDetails.SCHEDULED_TIME) < new Date() && !["POSTED", "SCHEDULED", "APPROVED", "REJECTED", "C", "S"].includes(viewingBatchState) && (
                                  <Typography component="span" color="error.main" fontWeight="bold" sx={{ ml: 1, fontSize: "0.75rem" }}>
                                    (Execution time lapsed)
                                  </Typography>
                                )}
                              </>
                            ) : "No"}
                          </Typography>
                        </Grid>
                        <Grid item xs={12} sm={2}>
                          <Typography variant="caption" color="text.secondary" display="block" gutterBottom>Download Voucher Batch</Typography>
                          <Button
                            variant="outlined"
                            size="small"
                            startIcon={<FileDownloadIcon />}
                            onClick={handleDownloadExcel}
                            disabled={!viewingBatchId}
                            tabIndex={-1} // <--- ADD THIS
                            onMouseDown={(e) => e.preventDefault()} // <--- ADD THIS: Prevents focus on click
                            sx={{
                              borderColor: "#217346",
                              color: "#217346",
                              "&:hover": { backgroundColor: "#e8f5e9", borderColor: "#1e6b40" },
                              // Completely nuke all focus states
                              "&:focus, &.Mui-focusVisible, &:active": {
                                outline: "none !important",
                                boxShadow: "none !important",
                                backgroundColor: "transparent !important",
                                border: "1px solid #217346 !important"
                              }
                            }}
                          >
                            Download Batch
                          </Button>
                        </Grid>
                      </Grid>
                    </Paper>
                  </Stack>
                ) : null}
              </Box>

              {/* --- HORIZONTAL TIMELINE --- */}
              <Box sx={{ p: 4, flexGrow: 1, display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
                <Typography variant="subtitle1" fontWeight="bold" color="text.primary" sx={{ mb: 3 }}>
                  Workflow Progress
                </Typography>


                {isFetchingSteps ? (
                  /* LAYOUT SHIFT FIX: Added minHeight: 120px so it holds space for the incoming timeline */
                  <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '120px', p: 3 }}>
                    <CircularProgress size={32} />
                  </Box>
                ) : dynamicSteps.length > 0 ? (
                  <Stepper
                    activeStep={(() => {
                      if (viewingBatch?.isExecutionFailed) return dynamicSteps.length - 1;
                      if (viewingBatchState === "POSTED" || viewingBatchState === "C") return dynamicSteps.length;
                      if (viewingBatchState === "SCHEDULED" || viewingBatchState === "WAITING FOR EXECUTION" || viewingBatchState === "APPROVED") return dynamicSteps.length - 1;
                      return viewingBatchOrder - 1;
                    })()}
                    alternativeLabel
                    connector={<BigConnector />}
                  >
                    {dynamicSteps.map((step, index) => {
                      // 1. ONLY mark as fully done if physically posted to the ledger
                      const isFullyDone = viewingBatchState === "POSTED" || viewingBatchState === "C";

                      let activeStepIndex = isFullyDone ? dynamicSteps.length : viewingBatchOrder - 1;

                      // 2. Pause on the final step if Scheduled/Waiting
                      if (viewingBatchState === "SCHEDULED" || viewingBatchState === "WAITING FOR EXECUTION" || viewingBatchState === "APPROVED") {
                        activeStepIndex = dynamicSteps.length - 1;
                      } else if (viewingBatch?.isExecutionFailed) {
                        activeStepIndex = dynamicSteps.length - 1;
                      }

                      const isCompleted = index < activeStepIndex;
                      const isCurrent = index === activeStepIndex;

                      let statusFlag = 'PENDING';
                      if (isCompleted) statusFlag = 'COMPLETED';
                      if (isCurrent && viewingBatchState === "REJECTED") statusFlag = 'REJECTED';

                      else if (isCurrent && viewingBatch?.isExecutionFailed) statusFlag = 'REJECTED';
                      else if (isCurrent) statusFlag = 'ACTIVE';

                      const historyItem = workflowHistory[index];
                      const executorStr = historyItem ? `${historyItem.ACTION_USER_NAME || 'System'} (${historyItem.LAST_ACTION_BY})` : null;

                      return (
                        <Step key={step.state} completed={isCompleted} active={isCurrent} sx={{ opacity: viewingBatchState === "REJECTED" && index > activeStepIndex ? 0.3 : 1 }}>
                          <StepLabel StepIconComponent={() => <BigWorkflowIcon statusFlag={statusFlag} label={step.designation} />}>

                            <Chip
                              label={getRoleDisplayName(step.role)}
                              size="small"
                              variant="outlined"
                              color={isCurrent && statusFlag !== 'REJECTED' ? "primary" : "default"}
                              sx={{ mt: 1, height: 22, fontSize: "0.65rem", fontWeight: "bold" }}
                            />

                            {(isCompleted || (isCurrent && viewingBatchState === "REJECTED") || (isCurrent && viewingBatch?.isExecutionFailed)) && (
                              <Box>
                                {historyItem && (
                                  <Typography variant="caption" display="block" color="success.main" sx={{ mt: 0.5, fontWeight: "bold" }}>
                                    {executorStr}
                                  </Typography>
                                )}

                                {historyItem && historyItem.stageDuration && historyItem.LAST_ACTION_BY !== "SYSTEM" && (
                                  <Typography variant="caption" display="block" color="text.secondary" sx={{ fontStyle: 'italic', fontSize: '0.65rem' }}>
                                    Pending Duration: {historyItem.stageDuration}
                                  </Typography>
                                )}
                              </Box>
                            )}

                            {isCurrent && viewingBatchState !== "REJECTED" && !viewingBatch?.isExecutionFailed && step.role !== "System" && (
                              <Typography variant="caption" display="block" color="text.secondary" fontWeight="bold" sx={{ mt: 0.5 }}>
                                Pending Since: {viewingBatchTimeSinceLastAction || "0h 0m"}
                              </Typography>
                            )}

                          </StepLabel>
                        </Step>
                      );
                    })}
                  </Stepper>
                ) : (
                  <Alert severity="info" sx={{ mt: 2 }}>Workflow configuration not found.</Alert>
                )}
              </Box>

              {/* --- WORKFLOW HISTORY --- */}
              <Box sx={{ p: 4, borderTop: "1px solid", borderColor: "divider" }}>
                {isFetchingSteps ? (
                  /* LAYOUT SHIFT FIX: Added minHeight: 200px so it holds space for the incoming history list */
                  <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '200px', p: 2 }}>
                    <CircularProgress size={32} />
                  </Box>
                ) : (
                  <>
                    <Typography variant="subtitle1" fontWeight="bold" color="text.primary" sx={{ mb: 3 }}>
                      Workflow History ({workflowHistory.length})
                    </Typography>
                    <Stack spacing={2.5}>
                      {workflowHistory.map((history, idx) => {

                        const isSystem = history.LAST_ACTION_BY === "SYSTEM";
                        const userName = history.ACTION_USER_NAME || (isSystem ? "Automated System" : "User");
                        const initial = isSystem ? "S" : userName.charAt(0).toUpperCase();

                        const isExecutionFail = history.LAST_ACTION?.toUpperCase().includes("FAILED");

                        // 1. Clean the action label
                        const rawAction = history.LAST_ACTION || "System Action";
                        const cleanAction = rawAction.replace(/\s*\(.*?\)\s*/g, '').trim();

                        // 2. Identify action types
                        const isSubmission = rawAction.toUpperCase().includes("SUBMIT");
                        const isFinalApproval = rawAction.toUpperCase().includes("FINAL") || rawAction.toUpperCase().includes("ACCEPT");

                        const isSchOverride = rawAction.toUpperCase().includes("SCH. OVERRIDE");
                        const isImmOverride = rawAction.toUpperCase().includes("IMM. OVERRIDE");
                        const isKeptSchedule = rawAction.toUpperCase().includes("(SCHEDULED)");

                        // 3. STRICT HISTORICAL SNAPSHOT: ONLY read from this specific history row!
                        // We NO LONGER check vidDetails here. This prevents the "overwriting" bug.
                        const historyRowTime = history.SCHEDULED_TIME;

                        return (
                          <Box key={idx} sx={{ display: 'flex', gap: 2, mb: 2 }}>
                            <Box>
                              <Avatar sx={{ bgcolor: isSystem ? 'secondary.main' : 'primary.main', width: 40, height: 40, fontWeight: 'bold', fontSize: '1rem' }}>
                                {initial}
                              </Avatar>
                            </Box>
                            <Box sx={{ flexGrow: 1, backgroundColor: "white", p: 2.5, borderRadius: 2, border: "1px solid", borderColor: isExecutionFail ? "#d32f2f" : isSystem ? "#9c27b0" : "#e2e8f0", borderLeftWidth: isExecutionFail || isSystem ? "4px" : "1px", boxShadow: "0 1px 2px 0 rgb(0 0 0 / 0.05)" }}>

                              <Box sx={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 1, mb: 1 }}>
                                <Typography variant="subtitle2" fontWeight="bold" color="text.primary">
                                  {userName} {isSystem ? "" : `(${history.LAST_ACTION_BY})`}
                                </Typography>
                                <Typography variant="caption" color="text.secondary">• {formatDateTime(history.LAST_ACTION_AT)}</Typography>
                              </Box>

                              <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1.5, flexWrap: "wrap", gap: 1 }}>
                                <Chip
                                  label={cleanAction}
                                  size="small"
                                  sx={{ height: 22, fontSize: "0.7rem", fontWeight: "bold", backgroundColor: isExecutionFail || rawAction.includes("Reject") ? "#fee2e2" : isSystem ? "#f3e8ff" : "#e0e7ff", color: isExecutionFail || rawAction.includes("Reject") ? "#991b1b" : isSystem ? "#7e22ce" : "#3730a3" }}
                                />

                                {/* --- STRICT ISOLATED CHIP LOGIC --- */}

                                {/* A. Logic exclusively for the Maker / Creator */}
                                {isSubmission ? (
                                  historyRowTime ? (
                                    <Chip label={`Scheduled for: ${formatDateTime(historyRowTime)}`} size="small" color="info" variant="outlined" sx={{ height: 22, fontSize: "0.7rem", fontWeight: "bold" }} />
                                  ) : (
                                    <Chip label="Immediate Execution" size="small" color="default" variant="outlined" sx={{ height: 22, fontSize: "0.7rem", fontWeight: "bold", backgroundColor: "#f1f5f9" }} />
                                  )
                                )

                                  // {/* B. Logic exclusively for the Final Approver */}
                                  : isFinalApproval && !isSystem ? (
                                    isSchOverride && historyRowTime ? (
                                      <Chip label={`Schedule Overridden to: ${formatDateTime(historyRowTime)}`} size="small" color="warning" variant="outlined" sx={{ height: 22, fontSize: "0.7rem", fontWeight: "bold" }} />
                                    ) : isImmOverride ? (
                                      <Chip label="Overridden to Immediate Execution" size="small" color="warning" variant="outlined" sx={{ height: 22, fontSize: "0.7rem", fontWeight: "bold" }} />
                                    ) : isKeptSchedule && historyRowTime ? (
                                      <Chip label={`Original Schedule Kept: ${formatDateTime(historyRowTime)}`} size="small" color="info" variant="outlined" sx={{ height: 22, fontSize: "0.7rem", fontWeight: "bold" }} />
                                    ) : (
                                      <Chip label="Immediate Execution" size="small" color="default" variant="outlined" sx={{ height: 22, fontSize: "0.7rem", fontWeight: "bold", backgroundColor: "#f1f5f9" }} />
                                    )
                                  ) : null}

                              </Stack>
                              <Typography variant="body2" color="text.primary" sx={{ whiteSpace: "pre-wrap", lineHeight: 1.6 }}>
                                {history.REMARKS || "Voucher Batch Created."}
                              </Typography>
                            </Box>
                          </Box>
                        );
                      })}

                    </Stack>
                  </>
                )}
              </Box>
            </Box>
          </DialogContent>

          {/* --- STICKY BOTTOM ACTION BAR (Where Delete lives now) --- */}
          <DialogActions sx={{ p: 3, borderTop: "1px solid", borderColor: "divider", backgroundColor: "white", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <Box sx={{ flexGrow: 1 }}>
              {/* Left aligned spacing element anchor */}
            </Box>
            <Stack direction="row" spacing={2} alignItems="center">

              {isViewingSelfCreated && isBatchDeletable && (
                <Button
                  variant="contained"
                  color="error"
                  startIcon={<DeleteIcon />}
                  onClick={() => {
                    setIsViewModalOpen(false); // Dismount detail modal view
                    handleOpenModal({ batchId: viewingBatchId }); // Initialize confirmation dialog box stages
                  }}
                  sx={{ minWidth: 160, fontWeight: 'bold', py: 1 }}
                >
                  Cancel Voucher Batch
                </Button>
              )}

            </Stack>
          </DialogActions>
        </Dialog>
      </Box>
    </Paper>
  );
}
import React, { useState, useEffect, useCallback, useRef } from "react";
import {
  Box,
  Typography,
  Grid,
  TextField,
  Button,
  Paper,
  IconButton,
  MenuItem,
  CircularProgress,
  Autocomplete,
  Tooltip,
  Checkbox,
  FormControlLabel,
  Chip,
  FormHelperText,
  Divider,
  Card,
  CardActionArea,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Collapse,
  Radio,
  Stack,
  Stepper,
  Popper,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  Popover

} from "@mui/material";
import { useNavigate } from "react-router-dom";
import AddIcon from "@mui/icons-material/Add";
import { Bolt as BoltIcon } from "@mui/icons-material";
import TaskAltIcon from '@mui/icons-material/TaskAlt';
import DeleteIcon from "@mui/icons-material/Delete";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import KeyboardArrowUpIcon from '@mui/icons-material/KeyboardArrowUp';
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';
import ScheduleIcon from "@mui/icons-material/Schedule";
import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import ArrowForwardIcon from "@mui/icons-material/ArrowForward";
import DateRangeTwoToneIcon from '@mui/icons-material/DateRangeTwoTone';
import EditNoteIcon from '@mui/icons-material/EditNote';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import TableChartIcon from '@mui/icons-material/TableChart';
import { debounce } from "lodash";
import useApi from "../../hooks/useApi";
import JournalBulkUpload from "./VoucherBulkUpload";
import useCustomSnackbar from "../../utils/useCustomSnackbar";
import dayjs from "dayjs";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import { TimePicker } from "@mui/x-date-pickers/TimePicker";
import { jpStyles } from "./VoucherStyle";
import { useSelector } from "react-redux";
import { findMenuById } from "../../utils/CommonUtilities";
const createNewRow = () => ({
  id: crypto.randomUUID ? crypto.randomUUID() : Date.now() + Math.random(),
  branch: null,
  currency: "",
  cgl: null,
  amount: "",
  txnType: "",
  remarks: "",
  productCode: "",
});

const formatDateTime = (dateVal) => {
  if (!dateVal) return "N/A";
  // Formats to: "19 Jun 2026, 12:58 pm"
  return dayjs(dateVal).format("DD MMM YYYY, hh:mm a").toLowerCase();
};

const ExpandableDescription = ({ description }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const desc = description || "No description provided for this voucher configuration.";
  const isLongDesc = desc.length > 100 || desc.split(/\r?\n/).length > 2;

  return (
    <Box sx={{ position: "relative" }}>
      <Collapse in={!isLongDesc || isExpanded} collapsedSize={20}>
        <Typography variant="body2" color="text.secondary" sx={{ whiteSpace: "pre-wrap", wordBreak: "break-word", overflowWrap: "break-word" }}>
          {desc}
        </Typography>
      </Collapse>
      {isLongDesc && (
        <Box sx={{ display: "flex", justifyContent: "flex-start", mt: 0.5 }}>
          <Button
            size="small"
            disableRipple
            onClick={(e) => {
              e.stopPropagation(); // Prevents the main row from collapsing when clicked
              setIsExpanded(!isExpanded);
            }}
            endIcon={isExpanded ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
            sx={{
              p: 0,
              minWidth: "auto",
              textTransform: "none",
              fontWeight: 600,
              color: "primary.main",
              "&:hover": { background: "transparent", textDecoration: "underline" }
            }}
          >
            {isExpanded ? "Read Less" : "Read More"}
          </Button>
        </Box>
      )}
    </Box>
  );
};


export default function VoucherPosting() {
  const { callApi } = useApi();
  const showSnackBar = useCustomSnackbar();
  const navigate = useNavigate();
  const menus = useSelector((state) => state.menus);
  const menuItems = useSelector((state) => state.menus.menus);
  const selectedMenuItem = menus.selectedMenuItem;
  const getStatusMenu = findMenuById(menuItems, 32);

  const user = useSelector((state) => state.auth?.user || null);

  const lockedVidRef = useRef(null);
  // --- INTERNAL WIZARD STATE ---
  const [activeStep, setActiveStep] = useState(0);
  const [entryMethod, setEntryMethod] = useState("MANUAL");

  const [submittedBatchId, setSubmittedBatchId] = useState(null);
  const [showSchedulePicker, setShowSchedulePicker] = useState(false);
  const [isApplyingSchedule, setIsApplyingSchedule] = useState(false);
  const [isExecutingImmediately, setIsExecutingImmediately] = useState(false);
  const [routingChoice, setRoutingChoice] = useState(null); // 'IMMEDIATE' or 'SCHEDULE'
  const [isFinalizing, setIsFinalizing] = useState(false);

  const [isConfirmModalOpen, setIsConfirmModalOpen] = useState(false);


  // --- VOUCHER TABLE STATE ---
  const [voucherList, setVoucherList] = useState([]);
  const [selectedVoucher, setSelectedVoucher] = useState(null);
  const [expandedRow, setExpandedRow] = useState(null); // Tracks which row is clicked open
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(5);

  const isMounted = useRef(true);
  const [currentUserId, setCurrentUserId] = useState(null);
  // --- STEPPER & WIZARD STATE ---
  const steps = ['Select Voucher', 'Entry Method', 'Data Entry'];

  const [groupedVouchers, setGroupedVouchers] = useState({});
  const [scheduledTime, setScheduledTime] = useState(() =>
    dayjs().add(30, 'minute').startOf('minute')
  );


  const [rows, setRows] = useState([createNewRow(), createNewRow()]);

  const handleResetRows = () => {
    setRows([createNewRow(), createNewRow()]);
    setBranchInputValues({});
    setCglInputValues({});
    setCommonBatchRemarks("");
    setChecked(false);
  };

  const [currencyOptions, setCurrencyOptions] = useState([]);

  const [isSubmitting, setIsSubmitting] = useState(false);

  const [commonBatchRemarks, setCommonBatchRemarks] = useState("");
  const [postingDate, setPostingDate] = useState("");

  const [checked, setChecked] = useState(false);

  const [isPageLoading, setIsPageLoading] = useState(true);
  const [isEodRunning, setIsEodRunning] = useState(false);

  // Replaces anything that is NOT an alphabet, number, or space
  const REMARKS_REPLACE_REGEX = /[^a-zA-Z0-9 ]/g;

  // Replaces anything that is NOT a digit (CGL is strictly digits)
  const CGL_REPLACE_REGEX = /[^0-9]/g;

  // Replaces anything that is NOT alphanumeric (Branch is strictly alphanumeric)
  const BRANCH_REPLACE_REGEX = /[^0-9]/g;
