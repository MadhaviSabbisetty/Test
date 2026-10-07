
              {/* --- WORKFLOW HISTORY --- */}
              <Box
                sx={{ p: 4, borderTop: "1px solid", borderColor: "divider" }}
              >
                {isFetchingSteps ? (
                  /* LAYOUT SHIFT FIX: Added minHeight: 200px so it holds space for the incoming history list */
                  <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '200px', p: 2 }}>
                    <CircularProgress size={32} />
                  </Box>
                ) : (
                  <>
                    <Typography variant="subtitle1" fontWeight="bold" color="text.primary" sx={jaStyles.timelineHeader}>
                      Workflow History ({workflowHistory.length})
                    </Typography>

                    <Stack spacing={2.5}>
                      {workflowHistory.map((history, idx) => {
                        const isSystem = history.LAST_ACTION_BY === "SYSTEM";
                        const userName = history.ACTION_USER_NAME || "User";
                        const initial = userName.charAt(0).toUpperCase();

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

                        // 3. Smart Fallback: Use history row time first; if submission, fallback to batch-level scheduled time
                        const effectiveScheduledTime = history.SCHEDULED_TIME || (isSubmission && vidDetails?.SCHEDULED_TIME ? vidDetails.SCHEDULED_TIME : null);

                        return (
                          <Box key={idx} sx={jaStyles.historyRowBox}>
                            <Box>
                              <Avatar sx={jaStyles.historyAvatar}>{initial}</Avatar>
                            </Box>
                            <Box sx={jaStyles.historyCard}>
                              <Box sx={jaStyles.historyHeaderBox}>
                                <Typography variant="subtitle2" fontWeight="bold" color="text.primary">
                                  {userName} {isSystem ? "" : `(${history.LAST_ACTION_BY})`}
                                </Typography>
                                <Typography variant="caption" color="text.secondary">
                                  • {formatDateTime(history.LAST_ACTION_AT)}
                                </Typography>
                              </Box>

                              <Stack direction="row" spacing={1} alignItems="center" sx={jaStyles.historyActionStack}>
                                <Chip
                                  label={cleanAction}
                                  size="small"
                                  sx={jaStyles.historyActionChip(isExecutionFail || rawAction.includes("Reject"))}
                                />

                                {/* --- FIX: SMART SCHEDULE CHIPS --- */}
                                {isFinalApproval && !isSystem ? (
                                  isSchOverride && effectiveScheduledTime ? (
                                    <Chip label={`Schedule Overridden to: ${formatDateTime(effectiveScheduledTime)}`} size="small" color="warning" variant="outlined" sx={jaStyles.historySchedChip} />
                                  ) : isImmOverride ? (
                                    <Chip label="Overridden to Immediate Execution" size="small" color="warning" variant="outlined" sx={jaStyles.historySchedChip} />
                                  ) : isKeptSchedule && effectiveScheduledTime ? (
                                    <Chip label={`Original Schedule Kept: ${formatDateTime(effectiveScheduledTime)}`} size="small" color="info" variant="outlined" sx={jaStyles.historySchedChip} />
                                  ) : (
                                    <Chip label="Immediate Execution" size="small" color="default" variant="outlined" sx={{ ...jaStyles.historySchedChip, backgroundColor: "#f1f5f9" }} />
                                  )
                                ) : (
                                  effectiveScheduledTime ? (
                                    <Chip label={`Scheduled for: ${formatDateTime(effectiveScheduledTime)}`} size="small" color="info" variant="outlined" sx={jaStyles.historySchedChip} />
                                  ) : isSubmission && !isSystem ? (
                                    <Chip label="Immediate Execution" size="small" color="default" variant="outlined" sx={{ ...jaStyles.historySchedChip, backgroundColor: "#f1f5f9" }} />
                                  ) : null
                                )}
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

            {/* --- STATE 2: UNIVERSAL ACTION SCREEN --- */}
            {isActionMode && (
              <Box sx={jaStyles.actionScreenBox}>
                <Typography variant="h5" fontWeight="bold" gutterBottom>
                  Process Workflow Action
                </Typography>
                <Typography
                  variant="body1"
                  color="text.secondary"
                  textAlign="center"
                  sx={jaStyles.actionSubtitle}
                >
                  You are currently processing the <strong>{viewingBatchState || "Review"}</strong> stage for Batch <strong>{viewingBatchId}</strong>.
                  Please select your decision and provide remarks below.
                </Typography>

                {/* ADDED: minHeight: 480 prevents the card from collapsing when "Reject" hides the scheduling inputs */}
                <Paper elevation={0} sx={jaStyles.actionCard}>
                  {/* 1. DECISION TOGGLE */}
                  <Typography variant="subtitle2" fontWeight="bold" color="text.primary" gutterBottom>
                    Action <Typography component="span" color="error">*</Typography>
                  </Typography>

                  {/* FIX: Check if the last action in history was an execution failure */}
                  {(() => {
                    const isExecutionFailed = workflowHistory.length > 0 && workflowHistory[workflowHistory.length - 1]?.LAST_ACTION === "Execution Failed";

                    return (
                      <Grid container spacing={2} sx={{ mb: 3 }}>
                        {isExecutionFailed ? (
                          <Grid item size={{ lg: 12, md: 12 }}>
                            <Box
                              onClick={() => setActionSelection("ACCEPTED")}
                              sx={{
                                p: 2, border: "2px solid", borderRadius: 2, cursor: "pointer", transition: "all 0.2s",
                                borderColor: actionSelection === "ACCEPTED" ? "warning.main" : "divider",
                                backgroundColor: actionSelection === "ACCEPTED" ? "warning.50" : "transparent",
                                display: "flex", alignItems: "center", gap: 1.5,
                                "&:hover": { borderColor: actionSelection !== "ACCEPTED" ? "warning.light" : "warning.main" }
                              }}
                            >
                              <ReplayIcon color={actionSelection === "ACCEPTED" ? "warning" : "action"} />
                              <Box>
                                <Typography fontWeight="bold" color={actionSelection === "ACCEPTED" ? "warning.main" : "text.primary"}>
                                  Retry Approval
                                </Typography>
                                <Typography variant="caption" color="text.secondary">Re-attempt batch execution (Reject is temporarily disabled)</Typography>
                              </Box>
                            </Box>
                          </Grid>
                        ) : (
                          <>
                            <Grid item size={{ lg: 6, md: 6 }}>
                              <Box
                                onClick={() => setActionSelection("ACCEPTED")}
                                sx={{
                                  p: 2, border: "2px solid", borderRadius: 2, cursor: "pointer", transition: "all 0.2s",
                                  borderColor: actionSelection === "ACCEPTED" ? "success.main" : "divider",
                                  backgroundColor: actionSelection === "ACCEPTED" ? "success.50" : "transparent",
                                  display: "flex", alignItems: "center", gap: 1.5,
                                  "&:hover": { borderColor: actionSelection !== "ACCEPTED" ? "success.light" : "success.main" }
                                }}
                              >
                                <CheckIcon color={actionSelection === "ACCEPTED" ? "success" : "action"} />
                                <Box>
                                  <Typography fontWeight="bold" color={actionSelection === "ACCEPTED" ? "success.main" : "text.primary"}>
                                    Approve Request
                                  </Typography>
                                  <Typography variant="caption" color="text.secondary">Move to next stage</Typography>
                                </Box>
                              </Box>
                            </Grid>
                            <Grid item size={{ lg: 6, md: 6 }}>
                              <Box
                                onClick={() => setActionSelection("REJECTED")}
                                sx={{
                                  p: 2, border: "2px solid", borderRadius: 2, cursor: "pointer", transition: "all 0.2s",
                                  borderColor: actionSelection === "REJECTED" ? "error.main" : "divider",
                                  backgroundColor: actionSelection === "REJECTED" ? "error.50" : "transparent",
                                  display: "flex", alignItems: "center", gap: 1.5,
                                  "&:hover": { borderColor: actionSelection !== "REJECTED" ? "error.light" : "error.main" }
                                }}
                              >
                                <CloseIcon color={actionSelection === "REJECTED" ? "error" : "action"} />
                                <Box>
                                  <Typography fontWeight="bold" color={actionSelection === "REJECTED" ? "error.main" : "text.primary"}>
                                    Reject Request
                                  </Typography>
                                  <Typography variant="caption" color="text.secondary">Terminate workflow</Typography>
                                </Box>
                              </Box>
                            </Grid>
                          </>
                        )}
                      </Grid>
                    );
                  })()}

                  {/* 2. REMARKS INPUT */}
                  <Typography
                    variant="subtitle2"
                    fontWeight="bold"
                    color="text.primary"
                    gutterBottom
                  >
                    Action Remarks{" "}
                    <Typography component="span" color="error">
                      *
                    </Typography>
                  </Typography>
                   <TextField
                    fullWidth
                    multiline
                    rows={1}
                    placeholder="Enter remarks..."
                    value={executorRemarks}
                    onChange={(e) => handleRemarksChange(e, setExecutorRemarks)}
                    disabled={isSubmitting}
                    // Only turns red if they started typing but haven't hit the 2 character minimum
                    error={executorRemarks.length > 0 && executorRemarks.trim().length < 2}
                    // Permanently displays the allowed rules
                    helperText="Allowed 2-30 chars: Alphabets, numbers, and single spaces only"
                    sx={jaStyles.actionTextField}
                  />


                  {/* 3. CONDITIONAL SCHEDULING (FINAL APPROVAL ONLY) */}
                  {viewingBatch?.isFinalApproval === "Y" && (
                    <Box sx={jaStyles.routingBoxWrapper}>
                      <Typography
                        variant="subtitle2"
                        fontWeight="bold"
                        color="text.primary"
                        gutterBottom
                        sx={{ pt: "3px" }}
                      >
                        Final Execution Routing
                      </Typography>

                      {vidDetails?.SCHEDULED_TIME && new Date(vidDetails.SCHEDULED_TIME) < new Date() && actionSelection !== "REJECTED" && (
                        <Alert
                          variant="filled"
                          severity="warning"
                          sx={{
                            mb: 4,
                            fontSize: '1rem',
                          }}
                        >
                          The original scheduled execution time has been passed. You can schedule it again otherwise voucher posting will be executed immediately on Submit/Approval.
                        </Alert>
                      )}

                      {actionSelection === "REJECTED" ? (
                        <Alert severity="info" sx={jaStyles.routingAlert}>
                          Execution scheduling is not applicable for Rejected
                          batches.
                        </Alert>
                      ) : (
                        <Box sx={jaStyles.routingInnerBox}>
                          <Box
                            sx={{
                              p: 2,
                              mb: 3,
                              backgroundColor: "#f8fafc",
                              borderRadius: 2,
                              borderLeft: "4px solid",
                              borderColor: "primary.main",
                            }}
                          >
                            <Typography
                              variant="caption"
                              color="text.secondary"
                              fontWeight="bold"
                              display="block"
                            >
                              REQUESTED BY CREATOR (
                              {vidDetails?.BATCH_CREATED_BY_NAME})
                            </Typography>
                            <Typography
                              variant="body2"
                              fontWeight="bold"
                              color={vidDetails?.SCHEDULED_TIME ? "primary.main" : "text.primary"}
                            >
                              {vidDetails?.SCHEDULED_TIME ? (
                                <>
                                  Scheduled for: {formatDateTime(vidDetails.SCHEDULED_TIME)}
                                  {new Date(vidDetails.SCHEDULED_TIME) < new Date() && !["POSTED", "SCHEDULED", "APPROVED", "REJECTED", "C", "S"].includes(viewingBatchState) && (
                                    <Typography component="span" color="error.main" fontWeight="bold" sx={{ ml: 1, fontSize: "0.75rem" }}>
                                      (Execution time lapsed)
                                    </Typography>
                                  )}
                                </>
                              ) : "Immediate Execution"}
                            </Typography>
                          </Box>
                          <Grid container spacing={2} sx={{ width: '100%', mt: 0.5, mx: 0 }}>
                            {/* Hide this entire block if the time has already passed */}
                            {!(vidDetails?.SCHEDULED_TIME && new Date(vidDetails.SCHEDULED_TIME) < new Date()) && (
                              <Grid item xs={12} md={6} sx={{ p: 0 }}>
                                <Box
                                  onClick={() => setScheduleOverrideType("KEEP")}
                                  sx={jaStyles.overrideOption(scheduleOverrideType === "KEEP", false)}
                                >
                                  <Box sx={jaStyles.overrideRadioBtn(scheduleOverrideType === "KEEP", false)} />
                                  <Box sx={{ minWidth: 0, flex: 1 }}>
                                    <Typography
                                      variant="subtitle2"
                                      fontWeight="bold"
                                      sx={{ whiteSpace: 'normal', wordBreak: 'break-word' }}
                                      color={scheduleOverrideType === "KEEP" ? "success.dark" : "text.primary"}
                                    >
                                      Keep Original Routing
                                    </Typography>
                                    <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.5 }}>
                                      Execute as scheduled by creator
                                    </Typography>
                                  </Box>
                                </Box>
                              </Grid>
                            )}

                            {/* Make Override take full width (md={12}) if the other option is hidden */}
                            <Grid item xs={12} md={(vidDetails?.SCHEDULED_TIME && new Date(vidDetails.SCHEDULED_TIME) < new Date()) ? 12 : 6} sx={{ p: 0 }}>
                              <Box
                                onClick={() => {
                                  setScheduleOverrideType("OVERRIDE");

                                  // If the creator scheduled a time AND it is still in the future, prepopulate the time picker with it.
                                  // Otherwise (if it was immediate, or the time already lapsed), default to +5 minutes from now.
                                  if (vidDetails?.SCHEDULED_TIME && dayjs(vidDetails.SCHEDULED_TIME).isAfter(dayjs())) {
                                    setFinalScheduleTime(dayjs(vidDetails.SCHEDULED_TIME).format("YYYY-MM-DDTHH:mm"));
                                  } else {
                                    setFinalScheduleTime(dayjs().add(5, 'minute').format("YYYY-MM-DDTHH:mm"));
                                  }
                                }}
                                sx={jaStyles.overrideOption(scheduleOverrideType === "OVERRIDE", true)}
                              >
                                <Box sx={jaStyles.overrideRadioBtn(scheduleOverrideType === "OVERRIDE", true)} />
                                <Box sx={{ minWidth: 0, flex: 1 }}>
                                  <Typography variant="subtitle2" fontWeight="bold" sx={{ whiteSpace: 'normal', wordBreak: 'break-word' }} color={scheduleOverrideType === "OVERRIDE" ? "warning.dark" : "text.primary"}>
                                    Override Schedule
                                  </Typography>
                                  <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.5 }}>
                                    Set a new execution time
                                  </Typography>
                                </Box>
                              </Box>
                            </Grid>
                          </Grid>
                          {scheduleOverrideType === "OVERRIDE" && (
                            <LocalizationProvider dateAdapter={AdapterDayjs}>
                              <Box
                                sx={{
                                  mt: 2,
                                  mb: 2,
                                  p: { xs: 2, sm: 3 }, // Responsive padding to fit nicely on smaller screens
                                  borderRadius: 2,
                                  border: "1px solid",
                                  borderColor: "divider",
                                  animation: "fadeIn 0.2s ease-in-out",
                                  backgroundColor: "white",
                                  width: "100%",
                                  boxSizing: "border-box", // CRITICAL: Ensures padding is calculated inside the 100% width
                                }}
                              >
                                <TimePicker
                                  label="New Execution Time (Today)"
                                  value={finalScheduleTime ? dayjs(finalScheduleTime) : null}
                                  onChange={(newValue) => {
                                    setFinalScheduleTime(newValue ? newValue.format("YYYY-MM-DDTHH:mm") : "");
                                  }}
                                  disablePast
                                  minTime={dayjs()} // Removed the 30-minute block
                                  maxTime={dayjs().endOf('day')}
                                  timeSteps={{ minutes: 1 }}
                                  slotProps={{
                                    popper: {
                                      placement: "top",
                                    },
                                    textField: {
                                      size: 'small',
                                      fullWidth: true,
                                      required: true,
                                      sx: {
                                        caretColor: 'transparent',
                                        '& .MuiFormHelperText-root': {
                                          whiteSpace: 'normal',
                                          wordBreak: 'break-word',
                                          marginInline: 0,
                                          mt: 1
                                        }
                                      },
                                      helperText: "Execution must be scheduled for a future time before 11:59 PM today.",
                                      onKeyDown: (e) => e.preventDefault(),
                                      onPaste: (e) => e.preventDefault(),
                                      inputProps: { readOnly: true }
                                    }
                                  }}
                                />
                              </Box>
                            </LocalizationProvider>
                          )}

                        </Box>
                      )}
                    </Box>
                  )}
                </Paper>
              </Box>
            )}
          </DialogContent>
          {/* --- STICKY BOTTOM ACTION BAR --- */}
          <DialogActions sx={jaStyles.stickyBottomBar}>
            {(() => {
              const isWorkflowFinished =
                viewingBatchState === "POSTED" ||
                viewingBatchState === "SCHEDULED" ||
                viewingBatchState === "APPROVED" ||
                viewingBatchState === "REJECTED";
              const isActionDisabled =
                isViewingSelfCreated || isWorkflowFinished;

              return (
                <>
                  <Box sx={jaStyles.flexGrowBox}>
                    {isViewingSelfCreated && !isWorkflowFinished && (
                      <Typography
                        variant="body2"
                        color="error.main"
                        fontWeight="bold"
                      >
                        * Security Violation: You cannot approve or reject your
                        own request.
                      </Typography>
                    )}
                    {isWorkflowFinished && (
                      <Typography
                        variant="body2"
                        color="text.secondary"
                        fontWeight="bold"
                      >
                        * Workflow processing is complete for this batch.
                      </Typography>
                    )}
                  </Box>

                  <Stack direction="row" spacing={2} alignItems="center">
                    {!isActionMode ? (
                      <Button
                        variant="contained"
                        color="primary"
                        disabled={isActionDisabled}
                        onClick={handleProceed}
                        sx={jaStyles.proceedBtn}
                      >
                        Proceed to Action
                      </Button>
                    ) : (
                      <>
                        <Button onClick={() => setIsActionMode(false)} sx={jaStyles.backBtn} disabled={isSubmitting}>

                          Back to Details
                        </Button>
                        <Button
                          variant="contained"
                          color={actionSelection === "ACCEPTED" ? "success" : "error"}

                          // 1. FIX THE DISABLED LOGIC
                          // Removed the '!scheduleOverrideType' check. 
                          // Now, it only disables if they explicitly chose "OVERRIDE" but left the time blank.
                          disabled={
                            !actionSelection ||
                            !isRemarksValid(executorRemarks) ||
                            isSubmitting ||
                            (actionSelection === "ACCEPTED" &&
                              viewingBatch?.isFinalApproval === "Y" &&
                              scheduleOverrideType === "OVERRIDE" && !finalScheduleTime)
                          }

                          // 2. FIX THE ONCLICK LOGIC
                          onClick={() => {
                            // Check if the original time has lapsed
                            const isLapsed = vidDetails?.SCHEDULED_TIME && new Date(vidDetails.SCHEDULED_TIME) < new Date();

                            // It is considered an "override" if they explicitly chose OVERRIDE, 
                            // OR if the time has lapsed (which forces the backend to override the old schedule to 'immediate')
                            const isOverriding =
                              actionSelection === "ACCEPTED" &&
                              viewingBatch?.isFinalApproval === "Y" &&
                              (scheduleOverrideType === "OVERRIDE" || isLapsed);

                            handleSubmitAction(actionSelection, isOverriding);
                          }}
                          sx={jaStyles.proceedBtn}
                        >
                          {isSubmitting ? (
                            <CircularProgress size={20} color="inherit" />
                          ) : (
                            "Confirm & Submit"
                          )}
                        </Button>
                      </>
                    )}
                  </Stack>
                </>
              );
            })()}
          </DialogActions>
        </Dialog>
      </Box>
    </Paper>
  );
}
import React, { useEffect, useState, useMemo } from "react";

import {
  Box, Paper, Typography, Button, TextField, Dialog,
  DialogTitle, DialogContent, DialogActions, Snackbar,
  Alert, Accordion, AccordionSummary, AccordionDetails,
  FormControl, InputLabel, Select, MenuItem, FormHelperText
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import BlockIcon from "@mui/icons-material/Block";
import { DataGrid } from "@mui/x-data-grid";
import useApi from "../../hooks/useApi";

import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import dayjs from "dayjs";
import Tooltip from "@mui/material/Tooltip";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import FileDownloadIcon from "@mui/icons-material/FileDownload";
import { alpha } from "@mui/material/styles";

const styles = {
  mainContainer: {
    padding: 3,
    height: "100%",
    width: "100%",
  },
  dataGridContainer: {
    height: "100%",
    minHeight: 0,
    width: "100%",
    backgroundColor: "background.paper",
    boxShadow: 2,
    borderRadius: 2,
    border: 1,
    borderColor: "divider",
    "& .MuiDataGrid-columnHeaders": {
      backgroundColor: "#f5f5f5 !important",
      fontWeight: 700,
      borderBottom: "2px solid rgba(88, 70, 159, 0.2)",
      position: "sticky",
      top: 0,
      zIndex: 1,
    },
    "& .MuiDataGrid-row:hover": {
      backgroundColor: (theme) => alpha(theme.palette.primary.main, 0.04),
    },

    "& .MuiDataGrid-menuIcon": {
      visibility: "visible !important",
      width: "auto !important",
    },
    "& .MuiDataGrid-iconButtonContainer": {
      visibility: "visible !important",
      width: "auto !important",
    },
  },
  // tableContainer: {
  //   maxHeight: 450,
  //   overflowY: "auto",
  //   border: 1,
  //   borderColor: "divider",
  //   mt: 1,
  // },
  fixedContentBox: {
    minHeight: 400,
    display: "flex",
    flexDirection: "column",
    justifyContent: "space-between",
  },
  tableHeaderCell: {
    fontWeight: "bold",
    backgroundColor: "#f5f6f8",
    color: "text.primary",
    whiteSpace: "nowrap",
    zIndex: 10,
  },
  stickyHeaderColumn: (leftOffset) => ({
    position: "sticky",
    left: leftOffset,
    top: 0,
    zIndex: 20,
    backgroundColor: "background.paper",
    borderBottom: 1,
    borderRight: 1,
    borderColor: "divider",
    fontWeight: "bold",
  }),
  stickyBodyColumn: (leftOffset) => ({
    position: "sticky",
    left: leftOffset,
    zIndex: 5,
    backgroundColor: "background.paper",
    borderRight: 1,
    borderColor: "divider",
  }),
  dialogTitle: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    pb: 1,
  },
  dialogActions: { p: 2, borderTop: 1, borderColor: "divider" },
  loadingContainer: {
    height: "100%",
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
  },
  helperText: {
    fontSize: "0.75rem",
    color: "text.secondary",
    marginTop: "4px",
  },
};


const ViewVoucherRequestsScreen = () => {
  const { callApi } = useApi();

  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);

  const [searchVID, setSearchVID] = useState("");

  const [searchStatus, setSearchStatus] = useState("");
  const [searchedStatus, setSearchedStatus] = useState("ACTIVE");
  const [searchCategory, setSearchCategory] = useState("");
  const [expanded, setExpanded] = useState(false);

  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [selectedRow, setSelectedRow] = useState(null);
  const [deleteRemark, setDeleteRemark] = useState("");
  const [isUsed, setIsused] = useState(true);
  const [remarkTouched, setRemarkTouched] = useState(false);


  const [descriptionPopupOpen, setDescriptionPopupOpen] =

    useState(false);

  const [selectedDescription, setSelectedDescription] =
    useState("");

  const [selectedIssues, setSelectedIssues] =
    useState("");

  const [searchDate, setSearchDate] =
    useState(null);


  const [snackbar, setSnackbar] = useState({
    open: false,
    message: "",
    severity: "success",
  });
  const [remarkPopupOpen, setRemarkPopupOpen] =
    useState(false);

  const [selectedRemark, setSelectedRemark] =
    useState("");




  const [voucherCategories, setVoucherCategories] = useState([]);
  const [voucherStatuses, setVoucherStatuses] = useState([]);
  const [vidError, setVidError] = useState("");
  const [remarkError, setRemarkError] = useState("");


  useEffect(() => {
    fetchVoucherCategories();
    fetchVoucherStatuses();
    fetchVouchers();
  }, []);


  const handleViewDescription = (
    description,
    issueCategories
  ) => {
    setSelectedDescription(
      description || "-"
    );
    setSelectedIssues(
      issueCategories ? issueCategories.split(",").map(item => item.trim()) : []
    );

    setDescriptionPopupOpen(
      true
    );
  };


  const handleViewRemark = (
    remark
  ) => {
    setSelectedRemark(
      remark || "-"
    );

    setRemarkPopupOpen(
      true
    );
  };


  const fetchVoucherCategories = async () => {

    try {
      const response = await callApi(
        "/VE/voucher-transactions/voucher-categories",
        null,
        "GET"
      );

      const data = Array.isArray(response)
        ? response
        : response?.data || [];

      setVoucherCategories(data);

    } catch (error) {
      console.error(error);
      setVoucherCategories([]);
    }
  };

  const fetchVoucherStatuses = async () => {
    try {
      const response = await callApi(
        "/VE/voucher-transactions/voucher-statuses",
        null,
        "GET"
      );

      const data = Array.isArray(response)
        ? response
        : response?.data || [];

      setVoucherStatuses(data);

    } catch (error) {
      console.error(error);
      setVoucherStatuses([]);
    }
  };

  const handleVIDChange = (e) => {
    const value = e.target.value.toUpperCase();

    if (value.length > 50) {
      setVidError("Maximum 50 characters allowed");
      setTimeout(() => {
        setVidError("")
      }, 1500)
      return;
    }


    if (
      value.length <= 3 &&
      !"VID".startsWith(value)
    ) {
      setVidError("Must start with VID");
      setTimeout(() => {
        setVidError("")
      }, 1500)
      return;
    }

    if (
      value.length > 3 &&
      !/^VID[0-9-]*$/.test(value)
    ) {
      setVidError(
        "Only numbers and hyphen allowed after VID"
      );
      setTimeout(() => {
        setVidError("")
      }, 1500)
      return;
    }

    setVidError("");
    setSearchVID(value);
  };





  const fetchVouchers = async () => {
    setSearchedStatus(searchStatus || "ACTIVE");
    setLoading(true);

    try {
      const payload = {};


      if (
        !searchVID &&
        !searchStatus &&
        !searchDate &&
        !searchCategory
      ) {
        payload.status = "ACTIVE";
      }

      if (searchVID) {
        payload.vid = searchVID;
      }

      if (searchStatus) {
        payload.status = searchStatus;
      }

      if (searchDate) {
        payload.date = dayjs(searchDate).format("YYYY-MM-DD");
      }

      if (searchCategory) {
        payload.categoryId = searchCategory;
      }

      console.log("Search Payload =>", payload);

      const response = await callApi(
        "/VE/voucher-transactions/search-vid",
        payload,
        "POST"
      );

      console.log("Search Response =>", response);

      const data = Array.isArray(response)
        ? response
        : response?.data || [];

      setRows(data);

    } catch (error) {
      console.error("Fetch Error =>", error);

      setSnackbar({
        open: true,
        message: "Failed to fetch vouchers",
        severity: "error",
      });

    } finally {
      setLoading(false);
    }
  };

  const handleExport = async () => {
    try {
      setLoading(true);

      const payload = {};


      if (
        !searchVID &&
        !searchStatus &&
        !searchDate &&
        !searchCategory
      ) {
        payload.status = "ACTIVE";
      }

      if (searchVID) {
        payload.vid = searchVID;
      }

      if (searchStatus) {
        payload.status = searchStatus;
      }

      if (searchDate) {
        payload.date = dayjs(searchDate).format("YYYY-MM-DD");
      }

      if (searchCategory) {
        payload.categoryId = searchCategory;
      }

      console.log("Export Payload =>", payload);

      const response = await callApi(
        "/VE/voucher-transactions/search-vid/export",
        payload,
        "POST",
        "blob",
        "application/json",
        {},
        false
      );

      const blob = new Blob([response.data], {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      });

      const url = window.URL.createObjectURL(blob);

      const link = document.createElement("a");
      link.href = url;
      link.download = "VoucherRequests.xlsx";

      document.body.appendChild(link);
      link.click();

      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);

    } catch (error) {
      console.error("Export Error =>", error);

      setSnackbar({
        open: true,
        message: "Export failed",
        severity: "error",
      });
    } finally {
      setLoading(false);
    }
  };
  const handleReset = async () => {
    setSearchVID("");
    setSearchStatus("");
    setSearchCategory("");
    setSearchDate(null);
    setSearchedStatus("ACTIVE");
    setVidError("");

    setLoading(true);

    try {
      const response = await callApi(
        "/VE/voucher-transactions/search-vid",
        {
          status: "ACTIVE",
        },
        "POST"
      );

      const data = Array.isArray(response)
        ? response
        : response?.data || [];

      setRows(data);

    } catch (error) {
      console.error("Reset Error =>", error);

      setSnackbar({
        open: true,
        message: "Failed to fetch vouchers",
        severity: "error",
      });

    } finally {
      setLoading(false);
    }
  };
  const handleDeleteClick = (row) => {
    setSelectedRow(row);
    setDeleteRemark("");
    setRemarkTouched(false)
    setRemarkError("")
    setDeleteDialogOpen(true);
  };

  const handleTerminate = async () => {
    try {
      const payload = {
        id: selectedRow?.id,
        terminatedBy: sessionStorage.getItem("userId"),
        remarks: deleteRemark,
      };

      await callApi(
        "/VE/voucher-transactions/terminate-vid",
        payload,
        "POST"
      );

      setDeleteDialogOpen(false);

      setSnackbar({
        open: true,
        message: `Voucher ${selectedRow.vid} terminated successfully`,
        severity: "success",
      });

      fetchVouchers();
    } catch (error) {
      setSnackbar({
        open: true,
        message: "Terminate failed",
        severity: "error",
      });
    }
  };


