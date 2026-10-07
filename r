import React, { useState, useEffect, useCallback, useRef } from "react";
import {
  Box,
  Typography,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  TextField,
  Button,
  CircularProgress,
  Tooltip,
  IconButton,
  Stack,
  Grid,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Checkbox,
  TablePagination,
  Alert,
  FormHelperText,
  Stepper,
  Step,
  StepLabel,
  StepConnector,
  stepConnectorClasses,
  styled,
  Avatar,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  ToggleButton,
  ToggleButtonGroup,
} from "@mui/material";

import { DataGrid, GridToolbar } from "@mui/x-data-grid";
import {
  Check as CheckIcon,
  Clear as ClearIcon,
  Visibility as VisibilityIcon,
  FileDownload as FileDownloadIcon,
  Close as CloseIcon,
  Add as AddIcon,
  Remove as RemoveIcon,
  Replay as ReplayIcon,
} from "@mui/icons-material";
import AccountCircleIcon from "@mui/icons-material/AccountCircle";
import InfoIcon from "@mui/icons-material/Info";
import { alpha } from "@mui/material/styles";
import useApi from "../../hooks/useApi";
import useCustomSnackbar from "../../utils/useCustomSnackbar";
import { jaStyles } from "./VoucherStyle";
import { TimePicker } from "@mui/x-date-pickers/TimePicker";
import { DateTimePicker } from "@mui/x-date-pickers/DateTimePicker";
import dayjs from "dayjs";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import { useSelector } from "react-redux";
const styles = {
  mainContainer: {
    padding: 3,
    height: "100%",
    width: "100%",
  },
  dataGridContainer: {
    height: 600,
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
      position: 'sticky',
      top: 0,
      zIndex: 1,
    },
    "& .MuiDataGrid-row:hover": {
      backgroundColor: (theme) => alpha(theme.palette.primary.main, 0.04),
    },
    // --- ADD THESE TWO BLOCKS TO KEEP THE 3 DOTS ALWAYS VISIBLE ---
    "& .MuiDataGrid-menuIcon": {
      visibility: "visible !important",
      width: "auto !important",
    },
    "& .MuiDataGrid-iconButtonContainer": {
      visibility: "visible !important",
      width: "auto !important",
    },
  },



  tableContainer: {
    maxHeight: 450,
    overflowY: "auto",
    border: 1,
    borderColor: "divider",
    mt: 1,
  },
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
    whiteSpace: "normal",
    wordBreak: "break-word",
    lineHeight: 1.2,
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

const CustomConnector = styled(StepConnector)(({ theme }) => ({
  [`&.${stepConnectorClasses.alternativeLabel}`]: {
    top: 12, // Exactly half of the 24px icon height
    left: "calc(-50% + 12px)",
    right: "calc(50% + 12px)",
  },
  [`&.${stepConnectorClasses.active}`]: {
    [`& .${stepConnectorClasses.line}`]: {
      borderColor: theme.palette.success.main,
    },
  },
  [`&.${stepConnectorClasses.completed}`]: {
    [`& .${stepConnectorClasses.line}`]: {
      borderColor: theme.palette.success.main,
    },
  },
  [`& .${stepConnectorClasses.line}`]: {
    borderColor: theme.palette.divider,
    borderTopWidth: 2,
    borderRadius: 1,
  },
}));

const BigConnector = styled(StepConnector)(({ theme }) => ({
  [`&.${stepConnectorClasses.alternativeLabel}`]: {
    top: 20, // Center of the 40px tall pill
    left: "calc(-50% + 80px)", // Offset by half the width of the 160px pill
    right: "calc(50% + 80px)",
  },
  [`&.${stepConnectorClasses.active}`]: {
    [`& .${stepConnectorClasses.line}`]: {
      borderColor: theme.palette.success.main,
    },
  },
  [`&.${stepConnectorClasses.completed}`]: {
    [`& .${stepConnectorClasses.line}`]: {
      borderColor: theme.palette.success.main,
    },
  },
  [`& .${stepConnectorClasses.line}`]: {
    borderColor: theme.palette.divider,
    borderTopWidth: 3,
    borderRadius: 1,
  },
}));

const BigWorkflowIcon = (props) => {
  const { statusFlag, label } = props;

  const baseStyle = {
    height: 40,
    minWidth: 160,
    borderRadius: 20, // Pill shape
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    gap: 1,
    px: 2,
    position: "relative",
    zIndex: 2,
    backgroundColor: "white",
  };

  // Info/Escalation Badge (Top Right)
  const InfoBadge = () => (
    <Box sx={jaStyles.infoBadge}>!</Box>
  );

  if (statusFlag === "COMPLETED") {
    return (
      <Box sx={jaStyles.bwfCompleted(baseStyle)}>
        <CheckIcon sx={{ fontSize: 18 }} />
        <Typography variant="body2" fontWeight="bold">{label}</Typography>
      </Box>
    );
  }
  if (statusFlag === "REJECTED") {
    return (
      <Box sx={jaStyles.bwfRejected(baseStyle)}>
        <CloseIcon sx={{ fontSize: 18 }} />
        <Typography variant="body2" fontWeight="bold">{label}</Typography>
      </Box>
    );
  }
  if (statusFlag === "ESCALATED" || statusFlag === "ACTIVE") {
    return (
      <Box sx={jaStyles.bwfEscalated(baseStyle)}>
        <Typography variant="body2" fontWeight="bold">{label}</Typography>
        <InfoBadge />
      </Box>
    );
  }

  // PENDING / FUTURE STEPS (Gray outline)
  return (
    <Box sx={jaStyles.bwfPending(baseStyle)}>
      <Typography variant="body2" fontWeight="bold">{label}</Typography>
    </Box>
  );
};

function CustomNoRowsOverlay() {
  return (
    <Stack sx={jaStyles.noRowsStack}>
      <InfoIcon sx={jaStyles.noRowsIcon} />
      <Typography variant="h6">No Pending Requests</Typography>
    </Stack>
  );
}

const formatDateTime = (dateVal) => {
  if (!dateVal) return "N/A";
  // Removing .toLowerCase() keeps DayJS's native capitalized months (e.g. 'Jun')
  return dayjs(dateVal).format("DD MMM YYYY, hh:mm a");
};

export default function VoucherAuthorization() {
  const { callApi } = useApi();
  const showSnackBar = useCustomSnackbar();
  const user = useSelector((state) => state.auth?.user || null);
  const isMounted = useRef(true);
  const [currentUserId, setCurrentUserId] = useState(null);
  const [loading, setLoading] = useState(true);
  const [rows, setRows] = useState([]);
  const [workflowStepsMap, setWorkflowStepsMap] = useState({});
  const [detailTotalCount, setDetailTotalCount] = useState(0);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submittingAction, setSubmittingAction] = useState(null);
  const [selectedBatch, setSelectedBatch] = useState(null);
  const [modalMode, setModalMode] = useState("ACCEPTED");
  const [executorRemarks, setExecutorRemarks] = useState("");
  const [isViewModalOpen, setIsViewModalOpen] = useState(false);
  const [batchDetails, setBatchDetails] = useState([]);
  const [viewingBatch, setViewingBatch] = useState(null);
  const [actionSelection, setActionSelection] = useState(null); // SINGLE DECLARATION, DEFAULT NULL
  const [isScheduleLapseModalOpen, setIsScheduleLapseModalOpen] = useState(false);
  const [newScheduleTime, setNewScheduleTime] = useState("");
  const [detailLoading, setDetailLoading] = useState(false);
  const [viewingBatchId, setViewingBatchId] = useState(null);
  const [viewingBatchCreatorId, setViewingBatchCreatorId] = useState(null);
  const [viewingBatchState, setViewingBatchState] = useState("");
  const [dynamicSteps, setDynamicSteps] = useState([]);
  const [dbRoleMap, setDbRoleMap] = useState({});
  const [isFetchingSteps, setIsFetchingSteps] = useState(false);
  const [vidDetails, setVidDetails] = useState(null);
  const [isFetchingVid, setIsFetchingVid] = useState(false);
  const [workflowHistory, setWorkflowHistory] = useState([]);
  const [isDescExpanded, setIsDescExpanded] = useState(false);
  const [viewingBatchOrder, setViewingBatchOrder] = useState(1);

  const [isActionMode, setIsActionMode] = useState(false); // Universal action screen state
  const [scheduleOverrideType, setScheduleOverrideType] = useState("KEEP");
  const [paginationModel, setPaginationModel] = useState({ page: 0, pageSize: 10 });
  const [totalRowCount, setTotalRowCount] = useState(0);
  const [filterModel, setFilterModel] = useState({ items: [] });
  const [finalScheduleTime, setFinalScheduleTime] = useState("");
  const handleDownloadExcel = async () => {
    if (!viewingBatchId) {
      showSnackBar("Error: Batch ID is missing!", "error");
      return;
    }

    try {
      showSnackBar("Downloading Excel...", "info");

      const response = await callApi(
        `/JS/voucher/download-batch/${viewingBatchId}`,
        null,
        "GET",
        "blob",
        null,
        {},
        false
      );

      const blobData = response.data || response;
      if (!blobData) {
        throw new Error("No data received from backend");
      }

      const blob = new Blob([blobData], {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      });
      const url = window.URL.createObjectURL(blob);

      const link = document.createElement("a");
      link.href = url;
      link.setAttribute("download", `Batch_${viewingBatchId}.xlsx`);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
      showSnackBar("Download complete.", "success");
    } catch (e) {
      console.error("Download failed:", e);
      let errorMsg = "Failed to download Excel file.";
      if (e.response && e.status === 404) {
        errorMsg = "API endpoint not found (404).";
      }
      showSnackBar(errorMsg, "error");
    }
  };

  const handleDirectRetry = async (row) => {
    setIsSubmitting(true);
    try {
      await callApi(
        "/JS/voucher/workflow/action",
        {
          batchId: row.batchId,
          status: "ACCEPTED",
          // CRITICAL FIX: Changed from 36 characters to 12 characters to pass your 30-char validation limit!
          remarks: "System Retry",
          overrideScheduleLapse: false,
          newScheduledTime: null,
        },
        "POST"
      );
      showSnackBar(`Batch ${row.batchId} execution triggered for retry!`, "success");
      fetchPendingBatches(); // Refresh table
    } catch (e) {
      showSnackBar(`Failed to retry Batch ${row.batchId}.`, "error");
    } finally {
      setIsSubmitting(false);
    }
  };

  const CustomWorkflowIcon = (props) => {
    const { statusFlag, iconLetter, label, isMini } = props;
    const showTextInside = isMini && (statusFlag === "ACTIVE" || statusFlag === "ESCALATED");

    const baseStyle = {
      height: 24, // Reduced size
      borderRadius: 12,
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      zIndex: 2,
      position: "relative",
      fontWeight: "bold",
      fontSize: "0.7rem",
      px: showTextInside ? 1.5 : 0,
      minWidth: showTextInside ? "auto" : 24,
      width: showTextInside ? "max-content" : 24,
      whiteSpace: "nowrap",
      boxSizing: "border-box", // Prevents borders from making circles larger
    };

    if (statusFlag === "COMPLETED") {
      return (
        <Box sx={{ ...baseStyle, backgroundColor: "success.main", color: "#fff" }}>
          <CheckIcon sx={{ fontSize: 16 }} />
        </Box>
      );
    }
    if (statusFlag === "ESCALATED") {
      return (
        <Box sx={{ ...baseStyle, backgroundColor: "white", border: "2px solid", borderColor: "warning.main", color: "warning.main" }}>
          {showTextInside ? label : "!"}
        </Box>
      );
    }
    if (statusFlag === "ACTIVE") {
      return (
        <Box sx={{ ...baseStyle, backgroundColor: "white", border: "2px solid", borderColor: "warning.main", color: "warning.main" }}>
          {showTextInside ? label : iconLetter}
        </Box>
      );
    }

    // PENDING / FUTURE STEPS (Now white with grey border to match the big stepper)
    return (
      <Box sx={{ ...baseStyle, backgroundColor: "white", border: "2px solid #e2e8f0", color: "text.disabled" }}>
        {iconLetter}
      </Box>
    );
  };

   useEffect(() => {
    isMounted.current = true;
    
    // Use the Redux 'user' object instead of parsing local storage!
    if (user && user.userId) {
      setCurrentUserId(String(user.userId));
      
      // Look for roleId or role or ROLE_ID based on your auth structure 
      const actualRoleId = user.roleId || user.role || user.ROLE_ID;
      if (actualRoleId) setDbRoleMap((prev) => ({ ...prev, currentRoleId: actualRoleId }));
    }
    
    return () => {
      isMounted.current = false;
    };
  }, [user]); 


 const filterAndWarn = (val) => {
    let filtered = val;
    let hasError = false;
    let msg = "Only alphanumeric characters and single spaces are allowed.";

    // 1. Strict Regex: Allows ONLY Alphabets, Numbers, and Spaces
    const invalidCharRegex = /[^a-zA-Z0-9 ]/g;

    if (invalidCharRegex.test(filtered)) {
      hasError = true;
      filtered = filtered.replace(invalidCharRegex, "");
    }

    // 2. Strip leading space and collapse double spaces
    const beforeSpaceTrim = filtered;
    filtered = filtered.replace(/^\s+/, "").replace(/\s{2,}/g, " ");

    if (beforeSpaceTrim !== filtered && !hasError) {
      hasError = true;
      msg = "Remark should not start with a space or contain consecutive spaces.";
    }

    if (hasError) {
      showSnackBar(msg, "warning");
    }

    return filtered;
  };

  const handleRemarksChange = (e, setter) => {
    const val = filterAndWarn(e.target.value);
    if (val.length <= 30) {
      setter(val);
    }
  };

  // Validation logic: Remarks are strictly compulsory (min 2, max 30)
  const isRemarksValid = (val) =>
    val.trim().length >= 2 && val.trim().length <= 30;

  const isViewingSelfCreated =
    currentUserId &&
    viewingBatchCreatorId &&
    String(currentUserId).trim().toUpperCase() ===
    String(viewingBatchCreatorId).trim().toUpperCase();

  const isLargeBatch = detailTotalCount > 2000;
  const fetchPendingBatches = useCallback(async () => {
    setLoading(true);
    try {
      // --- ADD: EXTRACT SEARCH FILTERS ---
      let batchIdSearch = "";
      let vidSearch = "";
      if (filterModel && filterModel.items) {
        filterModel.items.forEach(item => {
          if (item.field === 'batchId' && item.value) batchIdSearch = item.value;
          if (item.field === 'vid' && item.value) vidSearch = item.value;
        });
      }

      // --- ADD: PASS FILTERS TO API ---
      const response = await callApi(
        `/JS/voucher/checker/pending-approvals?page=${paginationModel.page}&size=${paginationModel.pageSize}&batchId=${encodeURIComponent(batchIdSearch)}&vid=${encodeURIComponent(vidSearch)}`,
        null,
        "GET"
      );

      // --- ADAPT: HANDLE PAGINATED RESPONSE ---
      let fetchedRows = [];
      let totalElements = 0;

      if (response && response.content) {
        fetchedRows = response.content;
        totalElements = response.totalElements;
      } else if (Array.isArray(response)) {
        fetchedRows = response;
        totalElements = response.length;
      }

      const uniqueWorkflows = [...new Set(fetchedRows.map((row) => row.approvalWorkflow).filter(Boolean))];

      const stepsMap = {};
      await Promise.all(
        uniqueWorkflows.map(async (wfCode) => {
          try {
            const stepsResponse = await callApi(`/JS/voucher/workflow/steps/${wfCode}`, null, "GET");
            stepsMap[wfCode] = Array.isArray(stepsResponse) ? stepsResponse : (stepsResponse?.data || []);
          } catch (e) {
            stepsMap[wfCode] = [{ state: "P", designation: "Creator" }, { state: "APPROVED", designation: "Approver" }];
          }
        })
      );

      if (isMounted.current) {
        setWorkflowStepsMap(stepsMap);
        setRows(fetchedRows);
        setTotalRowCount(totalElements);
      }
    } catch (err) {
      if (isMounted.current && err.name !== "CanceledError")
        showSnackBar("Failed to load requests", "error");
    } finally {
      if (isMounted.current) setLoading(false);
    }
  }, [callApi, showSnackBar, paginationModel, filterModel]); // <--- IMPORTANT: Added paginationModel and filterModel here

  useEffect(() => {
    const fetchRoles = async () => {
      try {
        // Fetch the { "9001": "TCSGLIF" } map from our new API
        const rolesData = await callApi(
          "/JS/voucher/active-roles",
          null,
          "GET"
        );
        if (rolesData && isMounted.current) {
          setDbRoleMap(rolesData);
        }
      } catch (err) {
        console.error("Failed to fetch roles mapping", err);
      }
    };
    fetchRoles();
  }, [callApi]);

  useEffect(() => {
    fetchPendingBatches();
  }, [fetchPendingBatches]);

  const fetchBatchDetailsPaginated = async (batchId, page, size) => {
    setDetailLoading(true);
    try {
      const res = await callApi(
        `/JS/voucher/by-batch-paginated/${batchId}?page=${page}&size=${size}`,
        null,
        "GET"
      );

      const contentArray = res?.content || (Array.isArray(res) ? res : []);

      if (contentArray && isMounted.current) {
        setDetailTotalCount(res?.totalElements || contentArray.length || 0);
        const journalGroups = new Map();

        contentArray.forEach((req, index) => {
          try {
            console.log("Backend Row Data in Authorization:", req);

            const jId = req.journalId || req.id || `UNKNOWN-${index}`;
            const prefix = String(jId).split("-")[0];

            const amountVal = parseFloat(req.amount || req.reqAmount) || 0;

            const row = {
              ...req,
              journalPrefix: prefix, // Save the distinct prefix
              amount: Math.abs(amountVal),
              transactionType: amountVal < 0 ? "Credit" : "Debit",
              branch: req.branchCode || req.reqBranchCode || "-",
              currency: req.currency || req.reqCurrency || "-",
              cgl: req.cgl || req.reqCgl || "-",
              remarks: req.narration || req.reqNarration || "-",
              product: req.productCode || req.reqProduct || "-",
            };

            // FIX: Group explicitly by Branch!
            if (!journalGroups.has(prefix)) journalGroups.set(prefix, []);
            journalGroups.get(prefix).push(row);
          } catch (e) {
            console.error("Failed to map row details:", req, e);
          }
        });

        setBatchDetails(Array.from(journalGroups.values()));
      }
    } catch (e) {
      if (isMounted.current) showSnackBar("Failed to load details", "error");
    } finally {
      if (isMounted.current) setDetailLoading(false);
    }
  };
  const handleOpenViewModal = async (row) => {
    // 1. CLEAN SLATE FIX: Wipe all old data BEFORE opening the modal so nothing flashes!
    setVidDetails(null);
    setDynamicSteps([]);
    setWorkflowHistory([]);
    setIsDescExpanded(false);

    // Reset all action screen states to guarantee a clean slate
    setIsActionMode(false);
    setActionSelection(null);
    setExecutorRemarks("");
    setScheduleOverrideType("KEEP");
    setFinalScheduleTime("");

    setViewingBatch(row);
    setViewingBatchId(row.batchId);
    setViewingBatchCreatorId(String(row.creatorId));
    setViewingBatchState(row.workflowState || "");
    setViewingBatchOrder(row.currentOrder || 1);

    // Open modal immediately to show the perfectly sized loading spinners
    setIsViewModalOpen(true);
    setIsFetchingSteps(true);
    setIsFetchingVid(true);

    try {
      const workflowCode = row.approvalWorkflow || "Workflow_3";

      const [stepsData, summaryData, historyData] = await Promise.all([
        callApi(`/JS/voucher/workflow/steps/${workflowCode}`, null, "GET"),
        callApi(`/JS/voucher/batch-summary/${row.batchId}`, null, "GET"),
        callApi(`/JS/voucher/workflow-history/${row.batchId}`, null, "GET"),
      ]);

      setDynamicSteps(Array.isArray(stepsData) ? stepsData : (stepsData?.data || []));
      setVidDetails(summaryData?.data || summaryData);
      setWorkflowHistory(Array.isArray(historyData) ? historyData : (historyData?.data || []));

    } catch (err) {
      showSnackBar("Failed to load batch details.", "error");
    } finally {
      setIsFetchingSteps(false);
      setIsFetchingVid(false);
    }
  };

  const handleDetailRowsPerPageChange = (event) => {
    const newSize = parseInt(event.target.value, 10);
    setDetailRowsPerPage(newSize);
    setDetailPage(0);
    fetchBatchDetailsPaginated(viewingBatchId, 0, newSize);
  };

  const getRoleDisplayName = (roleValue) => {
    if (!roleValue || roleValue === "null" || roleValue === "System")
      return "System";

    // Now it looks inside the dynamic DB map instead of a hardcoded one!
    return dbRoleMap[String(roleValue)] || roleValue;
  };
  const handleCloseViewModal = () => {
    // 1. Force blur on whatever is currently focused to prevent jumping
    if (document.activeElement instanceof HTMLElement) {
      document.activeElement.blur();
    }
    document.body.focus();

    // 2. SMOOTH CLOSE FIX: Set modal to false IMMEDIATELY to trigger MUI fade-out animation.
    setIsViewModalOpen(false);

    // 3. Wait 300ms for the animation to finish, THEN wipe the data memory.
    setTimeout(() => {
      setBatchDetails([]);
      setViewingBatch(null);
      setViewingBatchId(null);
      setViewingBatchCreatorId(null);
      setIsDescExpanded(false);
      setVidDetails(null);
      setDynamicSteps([]);
      setWorkflowHistory([]);

      // Reset Action UI states
      setIsActionMode(false);
      setActionSelection(null);
      setExecutorRemarks("");
      setScheduleOverrideType("KEEP");
      setFinalScheduleTime("");

      // Refresh the table behind the scenes
      fetchPendingBatches();
    }, 300);
  };


  const handleProceed = () => {
    setIsActionMode(true); // Open the universal action screen

    if (viewingBatch?.isFinalApproval === "Y") {
      // --- THE FIX: Only set defaults if the user hasn't touched the scheduling preferences yet ---
      if (scheduleOverrideType === "KEEP" && !finalScheduleTime) {
        const isLapsed = vidDetails?.SCHEDULED_TIME && new Date(vidDetails.SCHEDULED_TIME) < new Date();

        if (isLapsed) {
          setScheduleOverrideType(null);
          setFinalScheduleTime("");
        } else {
          setScheduleOverrideType("KEEP");
          if (vidDetails?.SCHEDULED_TIME) {
            setFinalScheduleTime(dayjs(vidDetails.SCHEDULED_TIME).format("YYYY-MM-DDTHH:mm"));
          } else {
            setFinalScheduleTime("");
          }
        }
      }
      // If scheduleOverrideType is already "OVERRIDE" or has values changed by user, we do NOTHING.
      // This leaves their choices completely untouched when they come back!
    }
  };


  const handleOpenModal = (batch, mode) => {
    setSelectedBatch(batch);
    setModalMode(mode);
    setExecutorRemarks("");
    setIsModalOpen(true);
  };

  const handleCloseModal = () => {
    if (isSubmitting) return;
    setIsModalOpen(false);
    setSelectedBatch(null);
  };

  const handleInitiateAccept = () => {
    // Check if the current user is the final approver from our updated API
    if (viewingBatch?.isFinalApproval === "Y") {
      setIsSchedulingMode(true);

      if (vidDetails?.SCHEDULED_TIME) {
        const dateObj = new Date(vidDetails.SCHEDULED_TIME);
        dateObj.setMinutes(dateObj.getMinutes() - dateObj.getTimezoneOffset());
        setFinalScheduleTime(dateObj.toISOString().slice(0, 16));
      } else {
        setFinalScheduleTime("");
      }
    } else {
      // Normal checker, skip the scheduling screen and just submit
      handleSubmitAction("ACCEPTED");
    }
  };
  const handleSubmitAction = async (actionStatus, overrideLapse = false) => {
    const statusToSubmit =
      typeof actionStatus === "string" ? actionStatus : modalMode;

    if (!isRemarksValid(executorRemarks)) {
      showSnackBar("Remarks are compulsory (2-30 chars).", "warning");
      return;
    }
    setSubmittingAction(statusToSubmit);
    setIsSubmitting(true);
    setModalMode(statusToSubmit);

    let formattedNewSchedule = null;
    const targetTime = isActionMode ? finalScheduleTime : newScheduleTime;

    if (overrideLapse && targetTime) {
      const today = dayjs();
      const scheduledDayjs = dayjs(targetTime);
      const finalSchedule = scheduledDayjs
        .year(today.year())
        .month(today.month())
        .date(today.date());

      const minAllowed = dayjs();
      const maxAllowed = dayjs().endOf('day');

      if (finalSchedule.isBefore(minAllowed)) {
        showSnackBar("Schedule time cannot be in the past.", "warning");
        setIsSubmitting(false);
        return;
      }
      if (finalSchedule.isAfter(maxAllowed)) {
        showSnackBar("Schedule time cannot exceed today's end of day (11:59 PM).", "warning");
        setIsSubmitting(false);
        return;
      }
      formattedNewSchedule = finalSchedule.format("YYYY-MM-DD HH:mm:ss");
    }

    try {
      const response = await callApi(
        "/JS/voucher/workflow/action",
        {
          batchId: viewingBatchId,
          status: statusToSubmit,
          remarks: executorRemarks.trim(),
          overrideScheduleLapse: overrideLapse,
          newScheduledTime: formattedNewSchedule,
        },
        "POST"
      );

      // --- FIX: Dynamic Success Message with Batch ID ---
      const actionName = statusToSubmit === "ACCEPTED" ? "approved" : "rejected";
      showSnackBar(
        `Batch ID ${viewingBatchId} has been successfully ${actionName}.`,
        "success"
      );

      setIsScheduleLapseModalOpen(false);
      setIsViewModalOpen(false);
      setIsActionMode(false);
      setActionSelection(null);
      setScheduleOverrideType("KEEP");
      setFinalScheduleTime("");
      setNewScheduleTime("");
      setExecutorRemarks("");
      fetchPendingBatches();
    } catch (e) {
      if (e.response && e.response.status === 409) {
        setIsScheduleLapseModalOpen(true);
        setIsSubmitting(false);
        return;
      }

      const serverMsg = e?.response?.data?.message
        || e?.response?.data?.error
        || (typeof e?.response?.data === 'string' ? e.response.data : null)
        || e?.message;

      // --- FIX: Dynamic Error Message with Batch ID ---
      const errorMsg = serverMsg
        ? `${serverMsg} (Batch ID: ${viewingBatchId})`
        : `Failed to process Batch ID ${viewingBatchId}. Please try again.`;

      showSnackBar(errorMsg, "error");
    } finally {
      setIsSubmitting(false);
      setSubmittingAction(null);
    }
  };
  const columns = [
    {
      field: "batchId",
      headerName: "Batch ID",
      width: 110,
      align: "center",
      headerAlign: "center",
      disableColumnMenu: false, // <--- ENABLED
      filterable: true,         // <--- ENABLED
      sortable: false,
    },
    {
      field: "vid",
      headerName: "VID",
      width: 170,
      align: "center",
      headerAlign: "center",
      disableColumnMenu: false, // <--- ENABLED
      filterable: true,         // <--- ENABLED
      sortable: false,
      renderCell: (p) => (
        <Typography
          variant="body2"
          fontWeight="bold"
          sx={{
            width: "100%",
            height: "100%",
            display: "flex",
            alignItems: "center", // Vertically centers text
            justifyContent: "center", // Horizontally centers text (redundant if align: "center" works, but safe)
          }}
        >
          {p.value || "N/A"}
        </Typography>
      ),
    },
    {
      field: "workflowState",
      headerName: "Workflow Progress",
      width: 360,
      align: "center",
      headerAlign: "center",
      disableColumnMenu: true,
      sortable: false,
      renderCell: (params) => {
        const state = params.value || "";
        const workflow = params.row.approvalWorkflow;
        const isRejected = state === "REJECTED" || params.row.batchStatus === "R";

        const steps = workflowStepsMap[workflow] || [];
        let activeStep = params.row.currentOrder ? params.row.currentOrder - 1 : 0;

        // NEW FIX: Force the UI to push the active step to the final Execution step if it failed!
        if (params.row.isExecutionFailed) {
          activeStep = steps.length - 1;
        } else if (state === "POSTED" || state === "SCHEDULED" || state === "C" || state === "S") {
          activeStep = steps.length;
        }

        if (activeStep === -1) activeStep = 0;

        const escalationHours = params.row.escalationTime || 24;
        const hoursPending = Math.abs(new Date() - new Date(params.row.requestDate)) / 36e5;
        const isEscalated = !isRejected && activeStep < steps.length && hoursPending > escalationHours;

        return (
          <Box sx={jaStyles.miniStepperBox}>
            <Stepper activeStep={activeStep} alternativeLabel connector={<CustomConnector />} sx={jaStyles.miniStepper}>
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
                  <Tooltip title={statusFlag === "ESCALATED" ? "Escalation Time Breached - Attention Required" : label} key={`${label}-${index}`} arrow>
                    {/* Passed params.row.isExecutionFailed to turn the line red */}
                    <Step completed={isCompleted} active={isCurrent} sx={jaStyles.miniStep(isRejected || params.row.isExecutionFailed, index, activeStep)}>
                      <StepLabel
                        StepIconComponent={() => (
                          <CustomWorkflowIcon
                            statusFlag={statusFlag}
                            iconLetter={label.charAt(0).toUpperCase()}
                            label={label}
                            isMini={true}
                          />
                        )}
                        sx={jaStyles.miniStepLabel}
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
      field: "creatorName",
      headerName: "Creator",
      width: 320,
      align: "center",
      headerAlign: "center",
      disableColumnMenu: true,
      sortable: false,
      renderCell: (p) => {
        const name = p.row.creatorName || p.row.creatorId;
        const id = p.row.creatorId;
        return (
          <Chip
            icon={<AccountCircleIcon />}
            label={`${name} (${id})`}
            size="small"
            variant="outlined"
          />
        );
      },
    },
    {
      field: "requestDate",
      headerName: "Submitted On",
      width: 230,
      align: "center",
      headerAlign: "center",
      disableColumnMenu: true,
      sortable: false,
      renderCell: (p) => formatDateTime(p.value),
    },

    {
      field: "requestCount",
      headerName: "No. of Entries",
      width: 220,
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
        
        // 1. PSO RETRY MODE: If it failed, the backend guarantees this is the PSO user. Just show Retry!
        if (params.row.isExecutionFailed) {
          return (
            <Tooltip title="Execution failed. PSO Retry Authorization." arrow>
               <Button
                 variant="contained"
                 size="small"
                 color="warning"
                 onClick={() => handleDirectRetry(params.row)}
                 sx={{ textTransform: "none", borderRadius: 2, boxShadow: 0, fontWeight: "bold" }}
                 startIcon={<ReplayIcon />}
               >
                 Retry
               </Button>
            </Tooltip>
          );
        }

        // 2. NORMAL MODE: Only return View & Action!
        return (
          <Button
            variant="contained"
            size="small"
            onClick={() => handleOpenViewModal(params.row)}
            sx={{ textTransform: "none", borderRadius: 2, boxShadow: 0 }}
          >
            View & Action
          </Button>
        );
      },
    },



  ];

  return (
    <Paper elevation={0} sx={jaStyles.mainPaper}>
      <Typography variant="h6" fontWeight="bold" gutterBottom color="primary.main" sx={{ ml: 6, mt: 2 }}>
        Voucher Authorization
      </Typography>
      <Box sx={styles.mainContainer}>
        <DataGrid
          rows={rows}
          columns={columns}
          loading={loading}
          getRowId={(row) => row.batchId}
          disableRowSelectionOnClick

          // --- Pagination & Filtering Server-Side ---
          paginationMode="server"
          rowCount={totalRowCount}
          paginationModel={paginationModel}
          onPaginationModelChange={setPaginationModel}

          filterMode="server"
          filterModel={filterModel}
          onFilterModelChange={setFilterModel}

          pageSizeOptions={[5, 10, 25]}
          disableColumnResize
          rowHeight={52}
          slots={{
            noRowsOverlay: CustomNoRowsOverlay,
            toolbar: GridToolbar
          }}
          sx={styles.dataGridContainer}
        />


        {/* --- Main Grid Approve/Reject Dialog --- */}

        <Dialog
          open={isModalOpen}
          onClose={handleCloseModal}
          fullWidth
          maxWidth="sm"
        >
          <DialogTitle>
            {modalMode === "ACCEPTED" ? "Accept" : "Reject"} Batch ID
          </DialogTitle>
          <DialogContent>
            <DialogContentText>
              Please provide the remarks to{" "}
              {modalMode === "ACCEPTED" ? "Accept" : "Reject"} Batch ID{" "}
              <strong>{selectedBatch?.batchId}</strong>
            </DialogContentText>
            <TextField
              autoFocus
              required
              margin="dense"
              label="Remarks"
              fullWidth
              variant="standard"
              value={executorRemarks}
              onChange={(e) => handleRemarksChange(e, setExecutorRemarks)}
              disabled={isSubmitting}
              inputProps={{ maxLength: 30 }}
              error={
                executorRemarks.length > 0 && !isRemarksValid(executorRemarks)
              }
            />
            <Typography sx={styles.helperText}>
              Allowed 2-30 chars (alphabets, numbers)
            </Typography>
          </DialogContent>
          <DialogActions sx={styles.dialogActions}>
            <Button onClick={handleCloseModal} disabled={isSubmitting}>
              Cancel
            </Button>
            <Button
              onClick={() => handleSubmitAction(false)}
              variant="contained"
              color={modalMode === "ACCEPTED" ? "success" : "error"}
              disabled={isSubmitting || !isRemarksValid(executorRemarks)}
            >
              {isSubmitting ? (
                <CircularProgress size={24} />
              ) : modalMode === "ACCEPTED" ? (
                "Accept"
              ) : (
                "Reject"
              )}
            </Button>
          </DialogActions>
        </Dialog>

        {/* --- Schedule Lapsed Edge Case Modal --- */}
        <Dialog
          open={isScheduleLapseModalOpen}
          onClose={() => setIsScheduleLapseModalOpen(false)}
          maxWidth="sm"
          fullWidth
        >
          <DialogTitle sx={jaStyles.lapseTitle}>
            {" "}
            Schedule Lapsed{" "}
          </DialogTitle>
          <DialogContent>
            <Alert severity="warning" sx={jaStyles.lapseAlert}>
              The originally scheduled execution time{" "}
              <strong>
                {vidDetails?.SCHEDULED_TIME ? formatDateTime(vidDetails.SCHEDULED_TIME) : "N/A"}
              </strong>{" "}
              for Batch ID <strong>{viewingBatchId}</strong> (Created by{" "}
              <strong>
                {viewingBatch?.creatorName || viewingBatchCreatorId}
              </strong>
              ) has already passed.
            </Alert>
            <Typography variant="body1" gutterBottom>
              Would you like to reschedule this batch for a future time, or
              would you prefer to post it immediately?
            </Typography>

            <Paper elevation={0} sx={jaStyles.lapsePaper}>
              <Typography variant="subtitle2" gutterBottom>
                Optional: Set New Schedule
              </Typography>
              <LocalizationProvider dateAdapter={AdapterDayjs}>
                <TimePicker
                  label="New Execution Time (Today)"
                  value={newScheduleTime ? dayjs(newScheduleTime) : null} 
                  onChange={(newValue) => {
                    setNewScheduleTime(newValue ? newValue.format("YYYY-MM-DDTHH:mm") : "");
                  }}
                  disablePast
                  minTime={dayjs()} // Removed the 31-minute block
                  maxTime={dayjs().endOf('day')}
                  timeSteps={{ minutes: 1 }}
                  slotProps={{
                    popper: { placement: "top" },
                    textField: {
                      size: 'small',
                      fullWidth: true,
                      required: true,
                      sx: {
                        caretColor: 'transparent',
                        '& .MuiFormHelperText-root': { whiteSpace: 'normal', wordBreak: 'break-word', marginInline: 0, mt: 1 }
                      },
                      helperText: "Execution must be scheduled for a future time before 11:59 PM today.",
                      onKeyDown: (e) => e.preventDefault(),
                      onPaste: (e) => e.preventDefault(),
                      inputProps: { readOnly: true }
                    }
                  }}
                />
              </LocalizationProvider>
              <FormHelperText>
                Leave blank to post immediately upon acceptance.
              </FormHelperText>
            </Paper>
          </DialogContent>
          <DialogActions sx={jaStyles.lapseActions}>
            <Button
              onClick={() => setIsScheduleLapseModalOpen(false)}
              color="inherit"
            >
              Cancel
            </Button>
            <Button
              onClick={() => handleSubmitAction(true)}
              variant="contained"
              color="primary"
              disabled={isSubmitting}
            >
              {isSubmitting ? (
                <CircularProgress size={20} color="inherit" />
              ) : (
                "Confirm & Proceed"
              )}
            </Button>
          </DialogActions>
        </Dialog>

        {/* --- View Details Modal --- */}
        <Dialog
          open={isViewModalOpen}
          onClose={handleCloseViewModal}
          fullWidth
          maxWidth="lg"
          disableAutoFocus={true}      // Prevents jumping to the first button on open
          disableEnforceFocus={true}   // Prevents forcing focus when clicking dead space
          disableRestoreFocus={true}   // Prevents ghost highlights when clicking outside
        >
          <DialogTitle sx={styles.dialogTitle}>
            <Typography variant="h6" fontWeight="bold">
              Batch Details: {viewingBatchId}
            </Typography>
            <IconButton onClick={handleCloseViewModal}>
              <CloseIcon />
            </IconButton>
          </DialogTitle>
          {/* ADDED: height: '75vh' explicitly locks the modal size so it NEVER jumps */}
          <DialogContent dividers sx={jaStyles.viewModalContent}>
            {/* --- STATE 1: STANDARD DETAILS (Hidden if Final Approver is scheduling) --- */}
            <Box sx={jaStyles.viewModalScrollBox(isActionMode)}>
              {/* ---  HEADER CARDS --- */}
              <Box
                sx={{
                  p: 4,
                  backgroundColor: "white",
                  borderBottom: "1px solid",
                  borderColor: "divider",
                }}
              >
                {/* --- DYNAMIC DETAILS GRIDS --- */}
                {isFetchingVid ? (
                  /* LAYOUT SHIFT FIX: Added minHeight: 300px so it holds space for the incoming cards */
                  <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '300px', p: 4 }}>
                    <CircularProgress size={32} />
                  </Box>
                ) : vidDetails && vidDetails.VID ? (
                  <Stack spacing={3} sx={jaStyles.stackFullWidth}>
                    {/* SECTION 1: VOUCHER DETAILS + EMBEDDED DESCRIPTION */}
                    <Paper
                      elevation={0}
                      sx={{
                        p: 3,
                        borderRadius: 2,
                        width: "100%",
                        border: "1px solid",
                        borderColor: "divider",
                        display: "flex",
                        flexDirection: "column",
                      }}
                    >
                      <Typography
                        variant="subtitle1"
                        fontWeight="bold"
                        color="text.primary"
                        gutterBottom
                        sx={{
                          borderBottom: "1px solid",
                          borderColor: "divider",
                          pb: 1,
                          mb: 2,
                        }}
                      >
                        Voucher Request ID : {vidDetails.VID}
                      </Typography>

                      <Typography
                        variant="caption"
                        color="text.secondary"
                        display="block"
                        gutterBottom
                      >
                        Voucher Description
                      </Typography>


                      <Box
                        sx={{
                          maxHeight: isDescExpanded ? 200 : 90,
                          mb: "24px",
                          overflowY: "auto",
                          pr: 1,
                          transition: "max-height 0.3s ease"
                        }}
                      >
                        <Typography
                          variant="body2"
                          color="text.primary"
                          sx={{ whiteSpace: "pre-wrap", lineHeight: 1.6 }}
                        >
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
                      {/* NEW: Issue Categories Column */}
                      <Grid item xs={12} sm={3} sx={{ mb: "24px" }}>
                        <Typography variant="caption" color="text.secondary" display="block" gutterBottom>
                          Issue Categories
                        </Typography>
                        <Typography variant="body1" fontWeight="600" color="primary.main">
                          {vidDetails.ISSUE_CATEGORIES || "N/A"}
                        </Typography>
                      </Grid>

                      <Grid container spacing={3}>
                        <Grid item xs={12} sm={3}>
                          <Typography variant="caption" color="text.secondary" display="block" gutterBottom>
                            Category
                          </Typography>
                          <Typography variant="body1" fontWeight="500">
                            {vidDetails.CATEGORY_NAME || "N/A"}
                          </Typography>
                        </Grid>


                        <Grid item xs={12} sm={3}>
                          <Typography variant="caption" color="text.secondary" display="block" gutterBottom>
                            Created By
                          </Typography>
                          <Stack direction="row" spacing={1} alignItems="center">
                            <AccountCircleIcon sx={{ fontSize: 20, color: "text.secondary" }} />
                            <Typography variant="body1" fontWeight="500">
                              {vidDetails.VID_CREATED_BY_NAME}{" "}
                              <Typography component="span" variant="caption" color="text.secondary">
                                ({vidDetails.VID_CREATOR_ID})
                              </Typography>
                            </Typography>
                          </Stack>
                        </Grid>

                        <Grid item xs={12} sm={3}>
                          <Typography variant="caption" color="text.secondary" display="block" gutterBottom>
                            Created At
                          </Typography>
                          <Typography variant="body1" fontWeight="500">
                            {vidDetails.VID_CREATED_AT
                              ? formatDateTime(vidDetails.VID_CREATED_AT)
                              : "N/A"}
                          </Typography>
                        </Grid>
                      </Grid>
                    </Paper>

                    {/* SECTION 2: BATCH EXECUTION DETAILS + DOWNLOAD BUTTON */}
                    <Paper
                      elevation={0}
                      sx={{
                        p: 3,
                        borderRadius: 2,
                        width: "100%",
                        border: "1px solid",
                        borderColor: "divider",
                        display: "flex",
                        flexDirection: "column",
                      }}
                    >
                      <Typography
                        variant="subtitle1"
                        fontWeight="bold"
                        color="text.primary"
                        gutterBottom
                        sx={{
                          borderBottom: "1px solid",
                          borderColor: "divider",
                          pb: 1,
                          mb: 2,
                        }}
                      >
                        Voucher Details
                      </Typography>

                      <Grid container spacing={3} alignItems="center">
                        <Grid item xs={12} sm={3}>
                          <Typography
                            variant="caption"
                            color="text.secondary"
                            display="block"
                            gutterBottom
                          >
                            Created By
                          </Typography>
                          <Stack
                            direction="row"
                            spacing={1}
                            alignItems="center"
                          >
                            <AccountCircleIcon
                              sx={{ fontSize: 20, color: "text.secondary" }}
                            />
                            <Typography variant="body1" fontWeight="500">
                              {vidDetails.BATCH_CREATED_BY_NAME}{" "}
                              <Typography
                                component="span"
                                variant="caption"
                                color="text.secondary"
                              >
                                ({vidDetails.BATCH_CREATOR_ID})
                              </Typography>
                            </Typography>
                          </Stack>
                        </Grid>

                        <Grid item xs={12} sm={2}>
                          <Typography
                            variant="caption"
                            color="text.secondary"
                            display="block"
                            gutterBottom
                          >
                            Created At
                          </Typography>
                          <Typography variant="body1" fontWeight="500">
                            {formatDateTime(vidDetails.BATCH_CREATED_AT)}
                          </Typography>

                        </Grid>

                        <Grid item xs={12} sm={2}>
                          <Typography
                            variant="caption"
                            color="text.secondary"
                            display="block"
                            gutterBottom
                          >
                            No. of Entries
                          </Typography>
                          <Typography
                            variant="body1"
                            fontWeight="600"
                            color="primary.main"
                          >
                            {viewingBatch?.requestCount || 0}
                          </Typography>
                        </Grid>

                        <Grid item xs={12} sm={3}>
                          <Typography
                            variant="caption"
                            color="text.secondary"
                            display="block"
                            gutterBottom
                          >
                            Is Execution Scheduled?
                          </Typography>
                          <Typography
                            variant="body1"
                            fontWeight="500"
                            color={vidDetails.SCHEDULED_TIME ? "info.main" : "text.primary"}
                          >
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
                          <Typography
                            variant="caption"
                            color="text.secondary"
                            display="block"
                            gutterBottom
                          >
                            Download Voucher Batch
                          </Typography>
                          <Button variant="outlined" size="small" startIcon={<FileDownloadIcon />} onClick={handleDownloadExcel} disabled={!viewingBatchId} disableFocusRipple={true} sx={jaStyles.downloadBtn}>

                            Download Batch
                          </Button>
                        </Grid>
                      </Grid>
                    </Paper>
                  </Stack>
                ) : null}
              </Box>
              {/* --- HORIZONTAL TIMELINE --- */}
              <Box sx={jaStyles.timelineWrapper}>
                <Typography variant="subtitle1" fontWeight="bold" color="text.primary" sx={jaStyles.timelineHeader}>
                  Workflow Progress
                </Typography>

                {viewingBatchState === "REJECTED" && !viewingBatch?.isExecutionFailed ? (
                  <Alert severity="error" variant="filled" sx={jaStyles.timelineErrorAlert}>
                    This batch was REJECTED and the workflow has been
                    terminated.
                  </Alert>
                ) : isFetchingSteps ? (
                  /* LAYOUT SHIFT FIX: Added minHeight: 120px */
                  <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '120px', p: 3 }}>
                    <CircularProgress size={32} />
                  </Box>
                ) : dynamicSteps.length > 0 ? (
                  <Stepper
                    activeStep={
                      viewingBatch?.isExecutionFailed
                        ? dynamicSteps.length - 1 // Force to final Execution step if failed
                        : (viewingBatchState === "POSTED" || viewingBatchState === "SCHEDULED" || viewingBatchState === "APPROVED" || viewingBatchState === "C" || viewingBatchState === "S")
                          ? dynamicSteps.length
                          : viewingBatchOrder - 1
                    }
                    alternativeLabel
                    connector={<BigConnector />}
                  >
                    {dynamicSteps.map((step, index) => {
                      const isFullyDone = viewingBatchState === "POSTED" || viewingBatchState === "SCHEDULED" || viewingBatchState === "APPROVED" || viewingBatchState === "C" || viewingBatchState === "S";

                      let activeStepIndex = isFullyDone ? dynamicSteps.length : viewingBatchOrder - 1;
                      if (viewingBatch?.isExecutionFailed) {
                        activeStepIndex = dynamicSteps.length - 1;
                      }

                      const isCompleted = index < activeStepIndex;
                      const isCurrent = index === activeStepIndex;

                      let statusFlag = "PENDING";
                      if (isCompleted) statusFlag = "COMPLETED";
                      if (isCurrent && viewingBatchState === "REJECTED") statusFlag = "REJECTED";
                      // NEW FIX: Make big stepper Red with X for Execution Failure
                      else if (isCurrent && viewingBatch?.isExecutionFailed) statusFlag = "REJECTED";
                      else if (isCurrent) statusFlag = "ACTIVE";

                      const historyItem = workflowHistory[index];
                      const executorStr = historyItem ? `${historyItem.ACTION_USER_NAME} (${historyItem.LAST_ACTION_BY})` : null;

                      return (
                        <Step key={step.state} completed={isCompleted} active={isCurrent} sx={jaStyles.miniStep(viewingBatchState === "REJECTED" || viewingBatch?.isExecutionFailed, index, activeStepIndex)}>
                          <StepLabel StepIconComponent={() => (<BigWorkflowIcon statusFlag={statusFlag} label={step.designation} />)}>
                            <Chip
                              label={getRoleDisplayName(step.role)}
                              size="small"
                              variant="outlined"
                              color={isCurrent && statusFlag !== 'REJECTED' ? "primary" : "default"}
                              sx={jaStyles.timelineChip}
                            />
                            {(isCompleted || (isCurrent && viewingBatchState === "REJECTED") || (isCurrent && viewingBatch?.isExecutionFailed)) && historyItem && (
                              <Stack spacing={0.5} sx={{ mt: 0.5 }}>
                                <Typography variant="caption" display="block" color="success.main" sx={{ fontWeight: "bold" }}>
                                  {executorStr}
                                </Typography>
                                {historyItem.stageDuration && (
                                  <Typography variant="caption" display="block" color="text.secondary" sx={{ fontStyle: 'italic', fontSize: '0.65rem' }}>
                                    Pending Duration: {historyItem.stageDuration}
                                  </Typography>
                                )}
                              </Stack>
                            )}
                            {isCurrent && viewingBatchState !== "REJECTED" && !viewingBatch?.isExecutionFailed && (
                              <Typography variant="caption" display="block" color="text.secondary" fontWeight="bold" sx={{ mt: 0.5 }}>
                                Pending Since: {viewingBatch?.timeSinceLastAction || "0h 0m"}
                              </Typography>
                            )}
                          </StepLabel>
                        </Step>
                      );
                    })}
                  </Stepper>
                ) : (
                  <Alert severity="info" sx={jaStyles.timelineInfoAlert}>
                    Workflow configuration not found.
                  </Alert>
                )}
              </Box>


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
