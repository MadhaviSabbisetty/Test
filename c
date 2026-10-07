import { alpha } from "@mui/material/styles";

// --- Main Containers ---
export const mainContainerStyle = {
  padding: 3,
  height: "100%",
  width: "100%",
};

export const loadingContainerStyle = {
  display: "flex",
  justifyContent: "center",
  alignItems: "center",
  minHeight: "500px",
  width: "100%",
};


export const uploadBoxStyle = {
  border: "2px dashed",
  borderColor: "primary.main",
  borderRadius: 2,
  p: 4,
  textAlign: "center",
  cursor: "pointer",
  bgcolor: "background.default",
  "&:hover": {
    bgcolor: "action.hover",
  },
};

export const controlPanelStyle = {
  p: 3,
  mb: 3,
  borderRadius: 2,
};

export const iconStyle = {
  fontSize: 60,
  color: "primary.main",
  mb: 1,
};

export const headerTextStyle = {
  fontWeight: 600,
};

export const subTextStyle = {
  color: "text.secondary",
  mt: 1,
};

// --- DataGrid Styles ---
export const dataGridContainerStyle = {

};

// --- Table Styles ---
export const tableContainerStyle = {
  marginTop: 2,
  borderRadius: 2,
  border: "1px solid",
  borderColor: "divider",
  overflow: "hidden",
  maxHeight: "60vh",
};

export const cellBorderStyle = {
  border: (theme) => `1px solid ${theme.palette.divider}`,
};

export const tableHeaderCellStyle = {
  ...cellBorderStyle,
  bgcolor: "#bcabebff",
  fontWeight: "bold",
  textAlign: "center",
};

// --- Sticky/Fixed Column Styles ---
export const stickyColumnCell = {
  position: "sticky",
  right: 0,
  backgroundColor: "background.paper", 
  zIndex: 1,
  boxShadow: "-2px 0px 5px rgba(0,0,0,0.05)",
  verticalAlign: "top",
  ...cellBorderStyle,
};

export const stickyBoxStyle = {
  position: "sticky",
  top: 0,
  zIndex: 2,
  backgroundColor: "background.paper",
  display: "flex",
  justifyContent: "center",
  alignItems: "center",
  paddingTop: "8px",
  paddingBottom: "8px",
  width: "100%",
};

// --- Dialog Styles ---
export const dialogTitleStyle = {
  m: 0,
  p: 2,
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  fontWeight: "bold",
};

export const dialogActionsStyle = {
  padding: 2,
  display: "flex",
  gap: 1,
  justifyContent: "flex-end",
};

export const dialogContentStyle = {
  paddingTop: "20px !important",
};

// --- Filter/Search Box Styles ---
export const filterContainerStyle = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  marginBottom: 2,
  flexWrap: "wrap",
  gap: 2,
};

export const searchFieldStyle = {
  minWidth: "250px",
  "& .MuiOutlinedInput-root": {
    borderRadius: "8px",
  },
};


export const jpStyles = {
// 1. Loading & EOD
loadingBox: { display: 'flex', justifyContent: 'center', alignItems: 'center', height: '80vh' },
eodBox: { display: 'flex', justifyContent: 'center', alignItems: 'top', height: '25vh', p: 2, marginBottom: '500px' },
eodPaper: { p: 6, textAlign: 'center', borderRadius: 4 },

// 2. Main Wrapper
mainPaper: { p: { xs: 1, md: 3 }, width: "100%" },

// 3. Step 0 (Table)
stepHeader: { ml: 1, mb: 2 },
tablePaper: { width: '100%', mb: 2, borderRadius: 2, overflow: 'hidden' },
tableContainer: { overflowX: "hidden" },
table: { tableLayout: "fixed" },
tableHead: { backgroundColor: "action.hover" },
ellipsisCell: { whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" },
collapseBox: { margin: 2, p: 2, borderRadius: 2 },
buttonRightAlign: { display: 'flex', justifyContent: 'flex-end', mt: 3 },
tableRow: (isSelected, isExpanded) => ({
  cursor: 'pointer',
  backgroundColor: isSelected ? "action.selected" : "inherit",
  '& > *': { borderBottom: isExpanded ? 'none' : '1px solid rgba(224, 224, 224, 1)' }
}),

// 4. Step 1 (Entry Method)
methodContainer: { display: "flex", flexDirection: "column" },
cardsWrapper: { display: "flex", flexWrap: "wrap", gap: 4, justifyContent: "center", alignItems: "center" },
methodCard: { width: 320, height: 260, borderRadius: 3, transition: "0.3s", "&:hover": { transform: "translateY(-5px)", boxShadow: 6 } },
methodCardArea: { height: "100%", p: 4, textAlign: "center", display: "flex", flexDirection: "column", justifyContent: "center" },
excelIcon: { fontSize: 60, mb: 2, color: "#217346" },
manualIcon: { fontSize: 60, mb: 2 },
backButtonBox: { display: 'flex', justifyContent: 'flex-start', mt: 6 },
   collapseGrid: {
  margin: 2, p: 2, borderRadius: 2, alignItems: "center", rowGap: 1.2
},
issueCategoryChip: {
  backgroundColor: "primary.light",
  size: "small",
  mr: "8px", mb: "5px"
},



// 5. Selected Voucher Component
svPaper: { p: 3, mb: 4, borderRadius: 1, border: "1px solid", borderColor: "divider" },
svHeaderBox: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderBottom: "1px solid", borderColor: "divider", pb: 1, mb: 2 },
svDescription: { whiteSpace: "pre-wrap", lineHeight: 1.6, fontWeight: 300 },

// 6. Step 2 (Data Entry Grid)
dataEntryPaper: { p: 2 },
gridContainer: { mt: 3, width: "100%" },
commonRemarksHelper: { color: "text.secondary", textAlign: "left", ml: 1.5 },
checkboxBox: { display: "flex", alignItems: "center", mt: 2 },
fullWidthSlot: { minWidth: "100%" },
captionText: { fontSize: "0.65rem", color: "text.secondary", mt: 0.5, ml: 1.2, display: "block", textAlign: "left" },
memoChipBox: { marginLeft: "2%", display: "flex", alignItems: "center" },
memoChip: { height: 20, fontSize: "0.7rem" },
deleteIconButton: { color: "inherit", "&:hover": { color: "error.main", backgroundColor: "action.hover" } },
rowDivider: { borderBottom: 1, borderColor: "divider", my: 2 },
actionButtonsWrapper: { mt: 4, display: "flex", justifyContent: "space-between", alignItems: "center" },

// 7. Step 3 (Success)
successBox: { display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '50vh' },
successPaper: { p: 5, maxWidth: 600, textAlign: 'center', borderRadius: 3 },
successIconProps: { fontSize: 80, color: 'success.main', mb: 2 },
scheduleContainer: { mt: 2, p: 3, backgroundColor: 'action.hover', borderRadius: 2, border: '1px dashed', borderColor: 'divider' },

//last one cansel
LocalizationProvider:{ display: 'flex', gap: 2, justifyContent: 'center' }
};



export const jaStyles = {
// 1. DataGrid & Layout Layouts
mainPaper: { bgcolor: "background.default" },
mainContainer: { padding: 3, height: "100%", width: "100%" },
dataGridContainer: {
  height: 600, width: "100%", backgroundColor: "background.paper", boxShadow: 2, borderRadius: 2, border: 1, borderColor: "divider",
  "& .MuiDataGrid-columnHeaders": { backgroundColor: (theme) => alpha(theme.palette.primary.main, 0.08), fontWeight: "bold", fontSize: "0.95rem" },
  "& .MuiDataGrid-row:hover": { backgroundColor: (theme) => alpha(theme.palette.primary.main, 0.04) },
},
noRowsStack: { height: "100%", alignItems: "center", justifyContent: "center" },
noRowsIcon: { fontSize: 48, color: "text.secondary", mb: 2 },

// 2. Dialog & Modals Common
dialogTitle: { display: "flex", justifyContent: "space-between", alignItems: "center", pb: 1 },
dialogActions: { p: 2, borderTop: 1, borderColor: "divider" },
helperText: { fontSize: "0.75rem", color: "text.secondary", marginTop: "4px" },

// 3. View Modal Specifics
viewModalContent: { p: 0, backgroundColor: "#f8fafc", height: "75vh", display: "flex", flexDirection: "column" },
viewModalScrollBox: (isActionMode) => ({ display: isActionMode ? "none" : "flex", flexDirection: "column", flexGrow: 1, overflowY: "auto" }),
viewModalHeaderCard: { p: 4, backgroundColor: "white", borderBottom: "1px solid", borderColor: "divider" },
detailCard: { p: 3, borderRadius: 2, width: "100%", border: "1px solid", borderColor: "divider", display: "flex", flexDirection: "column" },
detailCardTitle: { borderBottom: "1px solid", borderColor: "divider", pb: 1, mb: 2 },
descScrollBox: { maxHeight: 90, mb: "24px", overflowY: "auto", pr: 1 },
descText: { whiteSpace: "pre-wrap", lineHeight: 1.6 },

// 4. Workflow History
historySection: { p: 4, borderTop: "1px solid", borderColor: "divider" },
historyCard: { flexGrow: 1, backgroundColor: "white", p: 2.5, borderRadius: 2, border: "1px solid", borderColor: "#e2e8f0", boxShadow: "0 1px 2px 0 rgb(0 0 0 / 0.05)" },
historyAvatar: { bgcolor: "primary.main", width: 40, height: 40, fontWeight: "bold", fontSize: "1rem" },

// 5. Action Screen
actionScreenBox: { 
  p: { xs: 2, sm: 3, md: 4 }, // Scaled down padding to give internal components breathing room
  flexGrow: 1, 
  display: "flex", 
  flexDirection: "column", 
  alignItems: "center", 
  justifyContent: "flex-start", // Changed to flex-start to prevent strange vertical jumping
  animation: "fadeIn 0.3s ease-in-out", 
  backgroundColor: "#f8fafc", 
  overflowY: "auto",
  width: "100%"
},
actionCard: { 
  p: { xs: 2, sm: 3 }, // Controlled interior component padding
  width: "100%", 
  maxWidth: 650, 
  border: "1px solid", 
  borderColor: "divider", 
  borderRadius: 3, 
  backgroundColor: "white", 
  boxShadow: "0 4px 6px -1px rgb(0 0 0 / 0.1)",
  boxSizing: "border-box" // Guarantees padding stays structural
},
toggleGroup: { mb: 4, height: 48 },

// Dynamic Toggle Button Styles
toggleBtn: (actionSelection, targetValue) => {
  const isAccepted = targetValue === "ACCEPTED";
  const isSelected = actionSelection === targetValue;
  const mainColor = isAccepted ? "success.main" : "error.main";
  const darkColor = isAccepted ? "success.dark" : "error.dark";
  const hoverBg = isAccepted ? "#e8f5e9" : "#ffebee";

  return {
    fontWeight: "bold",
    color: isSelected ? "white !important" : mainColor,
    backgroundColor: isSelected ? `${mainColor} !important` : "transparent",
    borderColor: mainColor,
    "&:hover": { backgroundColor: isSelected ? `${darkColor} !important` : hoverBg },
  };
},

// 6. Routing / Scheduling override
overrideCardBox: { p: 2, mb: 3, backgroundColor: "#f8fafc", borderRadius: 2, borderLeft: "4px solid", borderColor: "primary.main" },

overrideOption: (isActive, isOverride) => {
  const mainColor = isOverride ? "warning.main" : "success.main";
  const bgColor = isOverride ? "#fffbeb" : "#f0fdf4";
  return {
    width: "100%",
    height: "100%", // Forces both grid items to match up perfectly
    p: 2, 
    borderRadius: 3, 
    border: "2px solid", 
    borderColor: isActive ? mainColor : "#e2e8f0",
    backgroundColor: isActive ? bgColor : "#ffffff", 
    cursor: "pointer", 
    display: "flex", 
    alignItems: "flex-start", 
    gap: 1.5, 
    boxSizing: "border-box",
    transition: "all 0.2s ease-in-out",
    boxShadow: isActive 
      ? `0 4px 12px ${isOverride ? "rgba(245, 158, 11, 0.12)" : "rgba(16, 185, 129, 0.12)"}` 
      : "0 1px 2px rgba(0,0,0,0.02)",
    "& *": { userSelect: "none" }, // Clean desktop tracking
    "&:hover": { 
      backgroundColor: isActive ? bgColor : "#f8fafc", 
      borderColor: isActive ? mainColor : "#cbd5e1"
    }
  };
},
overrideRadioBtn: (isActive, isOverride) => {
   const mainColor = isOverride ? "warning.main" : "success.main";
   return { 
     width: 18, 
     height: 18, 
     minWidth: 18, // Hard structural lock preventing compression
     maxWidth: 18,
     mt: 0.25, 
     borderRadius: "50%", 
     border: isActive ? "5px solid" : "2px solid", 
     borderColor: isActive ? mainColor : "#cbd5e1", 
     backgroundColor: "white",
     transition: "all 0.15s ease-in-out",
     boxSizing: "border-box"
   };
},
// 7. Sticky Bottom Bar
stickyBottomBar: { p: 3, borderTop: "1px solid", borderColor: "divider", backgroundColor: "white", display: "flex", justifyContent: "space-between", alignItems: "center" },


 // 1. BigWorkflowIcon Styles
infoBadge: { position: "absolute", top: -4, right: -4, width: 18, height: 18, borderRadius: "50%", backgroundColor: "warning.main", color: "white", display: "flex", alignItems: "center", justifyContent: "center", fontSize: "11px", fontWeight: "bold", border: "2px solid white" },
bwfCompleted: (baseStyle) => ({ ...baseStyle, border: "2px solid", borderColor: "success.main", backgroundColor: "#e8f5e9", color: "success.main" }),
bwfRejected: (baseStyle) => ({ ...baseStyle, border: "2px solid", borderColor: "error.main", backgroundColor: "#ffebee", color: "error.main" }),
bwfEscalated: (baseStyle) => ({ ...baseStyle, border: "2px solid", borderColor: "warning.main", color: "text.primary" }),
bwfPending: (baseStyle) => ({ ...baseStyle, border: "2px solid #e2e8f0", color: "text.disabled" }),

// 2. DataGrid Mini Stepper
miniStepperBox: { width: "100%", display: "flex", alignItems: "center", justifyContent: "flex-start", pl: 2, height: "100%" },
miniStepper: { width: "100%", mt: 0.5 },
miniStep: (isRejected, index, activeStep) => ({ px: 0, opacity: isRejected && index > activeStep ? 0.3 : 1 }),
miniStepLabel: { p: 0, m: 0 },

// 3. Modals & Loaders
centerProgress: { display: "flex", justifyContent: "center", p: 4 },
stackFullWidth: { width: "100%" },
lapseTitle: { color: "error.main", fontWeight: "bold" },
lapseAlert: { mb: 3 },
lapsePaper: { p: 2, mt: 3, backgroundColor: "action.hover", border: "1px dashed", borderColor: "divider" },
lapseActions: { p: 2 },

// 4. Details Section (Icons & Buttons)
accountIcon: { fontSize: 20, color: "text.secondary" },
downloadBtn: { 
  borderColor: "#217346", 
  color: "#217346", 
  outline: "none !important", // Force remove browser outlines
  "&:hover": { 
    backgroundColor: "#e8f5e9", 
    borderColor: "#1e6b40" 
  },
  // Nuke all ghost focus states MUI tries to apply
  "&:focus": { 
    outline: "none", 
    backgroundColor: "transparent" 
  },
  "&.Mui-focusVisible": { 
    outline: "none", 
    backgroundColor: "transparent", 
    boxShadow: "none" 
  },
  "&:active": {
    outline: "none"
  }
},

// 5. Timeline Section
timelineWrapper: { p: 4, flexGrow: 1, display: "flex", flexDirection: "column", justifyContent: "center" },
timelineHeader: { mb: 3 },
timelineErrorAlert: { mt: 2, fontWeight: "bold" },
timelineProgressBox: { display: "flex", justifyContent: "center", p: 3 },
timelineChip: { mt: 1, height: 22, fontSize: "0.65rem", fontWeight: "bold" },
timelinePendingText: { mt: 1 },
timelineInfoAlert: { mt: 2 },

// 6. History Section
historyProgressBox: { display: "flex", justifyContent: "center", p: 2 },
historyRowBox: { display: "flex", gap: 2 },
historyHeaderBox: { display: "flex", alignItems: "center", flexWrap: "wrap", gap: 1, mb: 1 },
historyActionStack: { mb: 1.5, flexWrap: "wrap", gap: 1 },
historyActionChip: (isReject) => ({
  height: 22, fontSize: "0.7rem", fontWeight: "bold",
  backgroundColor: isReject ? "#fee2e2" : "#e0e7ff",
  color: isReject ? "#991b1b" : "#3730a3",
}),
historySchedChip: { height: 22, fontSize: "0.7rem", fontWeight: "bold" },

// 7. Action Screen additions
actionSubtitle: { mb: 4, maxWidth: 500 },
actionTextField: { mb: 2 },
routingBoxWrapper: { },
routingAlert: { mt: 1, py: 0 },
routingInnerBox: { mt: 2 },

// 8. Sticky Bottom Action Buttons
flexGrowBox: { flexGrow: 1 },
proceedBtn: { minWidth: 160, fontWeight: "bold", py: 1  },
backBtn: { fontWeight: "bold", color: "text.secondary" },
};import React, { useState, useEffect, useCallback, useRef } from "react";
import {
  Box,
  Typography,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
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
  Stepper,
  Step,
  StepLabel,
  StepConnector,
  stepConnectorClasses,
  styled,
  Avatar,
  Alert,
} from "@mui/material";
import { DataGrid, GridToolbar } from "@mui/x-data-grid";
import {
  DeleteSweep as DeleteSweepIcon,
  Visibility as VisibilityIcon,
  FileDownload as FileDownloadIcon,
  Close as CloseIcon,
} from "@mui/icons-material";
import CheckIcon from "@mui/icons-material/Check";
import AccountCircleIcon from "@mui/icons-material/AccountCircle";
import InfoIcon from "@mui/icons-material/Info";
import DeleteIcon from "@mui/icons-material/Delete";
import { alpha } from "@mui/material/styles";
import useApi from "../../hooks/useApi";
import useCustomSnackbar from "../../utils/useCustomSnackbar";
import dayjs from "dayjs";
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
};
const formatCurrency = (val) => {
  if (val === null || val === undefined || val === "") return "0.0000";
  const strVal = String(val).trim();
  const parts = strVal.split(".");
  let intPart = parts[0];
  let decPart = (parts[1] || "").padEnd(4, "0").substring(0, 4);
  intPart = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return `${intPart}.${decPart}`;
};
const getStatusChipColor = (status) => {
  switch (status) {
    case "ACCEPTED":
      return "success";
    case "REJECTED":
      return "error";
    default:
      return "warning";
  }
};
const getTxnTypeColor = (txnType) =>
  txnType === "Credit" ? "error" : "success";

const CustomConnector = styled(StepConnector)(({ theme }) => ({
  [`&.${stepConnectorClasses.alternativeLabel}`]: {
    top: 12,
    left: 'calc(-50% + 12px)',
    right: 'calc(50% + 12px)',
  },
  [`&.${stepConnectorClasses.active}`]: {
    [`& .${stepConnectorClasses.line}`]: { borderColor: theme.palette.success.main },
  },
  [`&.${stepConnectorClasses.completed}`]: {
    [`& .${stepConnectorClasses.line}`]: { borderColor: theme.palette.success.main },
  },
  [`& .${stepConnectorClasses.line}`]: {
    borderColor: theme.palette.divider,
    borderTopWidth: 2,
    borderRadius: 1,
  },
}));


const BigConnector = styled(StepConnector)(({ theme }) => ({
  [`&.${stepConnectorClasses.alternativeLabel}`]: {
    top: 20,
    left: 'calc(-50% + 80px)',
    right: 'calc(50% + 80px)',
  },
  [`&.${stepConnectorClasses.active}`]: {
    [`& .${stepConnectorClasses.line}`]: { borderColor: theme.palette.success.main },
  },
  [`&.${stepConnectorClasses.completed}`]: {
    [`& .${stepConnectorClasses.line}`]: { borderColor: theme.palette.success.main },
  },
  [`& .${stepConnectorClasses.line}`]: {
    borderColor: theme.palette.divider,
    borderTopWidth: 3,
    borderRadius: 1,
  },
}));

const CustomWorkflowIcon = (props) => {
  const { statusFlag, iconLetter, label, isMini } = props;
  const showTextInside = isMini && (statusFlag === "ACTIVE" || statusFlag === "ESCALATED");

  const baseStyle = {
    height: 24, // Smaller, consistent size
    borderRadius: 12,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    zIndex: 2,
    position: "relative",
    fontWeight: "bold",
    fontSize: "0.7rem",
    px: showTextInside ? 1.2 : 0,
    minWidth: showTextInside ? "auto" : 24,
    width: showTextInside ? "max-content" : 24,
    whiteSpace: "nowrap",
    boxSizing: "border-box", // Strictly ensures borders don't bloat the circle making them unequal
  };

  if (statusFlag === "COMPLETED") {
    return (
      <Box sx={{ ...baseStyle, backgroundColor: "success.main", border: "2px solid", borderColor: "success.main", color: "#fff" }}>
        <CheckIcon sx={{ fontSize: 16 }} />
      </Box>
    );
  }
  if (statusFlag === "REJECTED") {
    return (
      <Box sx={{ ...baseStyle, backgroundColor: "error.main", border: "2px solid", borderColor: "error.main", color: "#fff" }}>
        <CloseIcon sx={{ fontSize: 16 }} />
      </Box>
    );
  }
  if (statusFlag === "ESCALATED") {
    return (
      <Box sx={{ ...baseStyle, backgroundColor: "white", border: "2px solid", borderColor: "warning.main", color: "warning.main" }}>
        {showTextInside ? `${label} !` : "!"}
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

  // PENDING / FUTURE STEPS
  return (
    <Box sx={{ ...baseStyle, backgroundColor: "white", border: "2px solid #e2e8f0", color: "text.disabled" }}>
      {iconLetter}
    </Box>
  );
};


const BigWorkflowIcon = (props) => {
  const { statusFlag, label } = props;
  const baseStyle = {
    height: 40,
    minWidth: 160,
    borderRadius: 20,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    gap: 1,
    px: 2,
    position: "relative",
    zIndex: 2,
    backgroundColor: "white",
  };
  const InfoBadge = () => (
    <Box
      sx={{
        position: "absolute",
        top: -4,
        right: -4,
        width: 18,
        height: 18,
        borderRadius: "50%",
        backgroundColor: "warning.main",
        color: "white",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: "11px",
        fontWeight: "bold",
        border: "2px solid white",
      }}
    >
      !
    </Box>
  );

  if (statusFlag === "COMPLETED")
    return (
      <Box
        sx={{
          ...baseStyle,
          border: "2px solid",
          borderColor: "success.main",
          backgroundColor: "#e8f5e9",
          color: "success.main",
        }}
      >
        <CheckIcon sx={{ fontSize: 18 }} />
        <Typography variant="body2" fontWeight="bold">
          {label}
        </Typography>
      </Box>
    );
  if (statusFlag === "REJECTED")
    return (
      <Box
        sx={{
          ...baseStyle,
          border: "2px solid",
          borderColor: "error.main",
          backgroundColor: "#ffebee",
          color: "error.main",
        }}
      >
        <CloseIcon sx={{ fontSize: 18 }} />
        <Typography variant="body2" fontWeight="bold">
          {label}
        </Typography>
      </Box>
    );
  if (statusFlag === "ESCALATED" || statusFlag === "ACTIVE")
    return (
      <Box
        sx={{
          ...baseStyle,
          border: "2px solid",
          borderColor: "warning.main",
          color: "text.primary",
        }}
      >
        <Typography variant="body2" fontWeight="bold">
          {label}
        </Typography>
        <InfoBadge />
      </Box>
    );
  return (
    <Box
      sx={{ ...baseStyle, border: "2px solid #e2e8f0", color: "text.disabled" }}
    >
      <Typography variant="body2" fontWeight="bold">
        {label}
      </Typography>
    </Box>
  );
};

const calculateTimeElapsed = (startDateString) => {
  if (!startDateString) return "N/A";
  const diffMins = Math.floor(
    Math.abs(new Date() - new Date(startDateString)) / 60000
  );
  if (diffMins === 0) return "Just now";
  if (diffMins < 60) return `${diffMins} min`;
  const hours = Math.floor(diffMins / 60);
  return diffMins % 60 > 0 ? `${hours} hr ${diffMins % 60} min` : `${hours} hr`;
};

function CustomNoRowsOverlay() {
  return (
    <Stack height="100%" alignItems="center" justifyContent="center">
      {" "}
      <InfoIcon sx={{ fontSize: 48, color: "text.secondary", mb: 2 }} />{" "}
      <Typography variant="h6">No Requests Found</Typography>{" "}
    </Stack>
  );
}

const formatDateTime = (dateVal) => {
  if (!dateVal) return "N/A";
  // Removing .toLowerCase() keeps DayJS's native capitalized months (e.g. 'Jun')
  return dayjs(dateVal).format("DD MMM YYYY, hh:mm a");
};

const handleBlurAll = () => {
  // This finds any element currently holding focus and forces it to lose focus
  if (document.activeElement instanceof HTMLElement) {
    document.activeElement.blur();
  }
};

export default function VoucherPostingStatus() {
  const { callApi } = useApi();
  const showSnackBar = useCustomSnackbar();
  const isMounted = useRef(true);
  const user = useSelector((state) => state.auth?.user || null);

  const [currentUserId, setCurrentUserId] = useState(null);

  const [loading, setLoading] = useState(true);
  const [rows, setRows] = useState([]);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [selectedBatch, setSelectedBatch] = useState(null);
  const [paginationModel, setPaginationModel] = useState({ page: 0, pageSize: 10 });
  const [totalRowCount, setTotalRowCount] = useState(0);
  const [filterModel, setFilterModel] = useState({ items: [] });
  // View/Paginate States
  const [isViewModalOpen, setIsViewModalOpen] = useState(false);
  const [viewingBatchId, setViewingBatchId] = useState(null);
  const [viewingBatch, setViewingBatch] = useState(null);
  const [viewingBatchState, setViewingBatchState] = useState("");
  const [viewingBatchCreatorId, setViewingBatchCreatorId] = useState(null);
  const [viewingBatchRequestDate, setViewingBatchRequestDate] = useState(null);
  const [viewingBatchRequestCount, setViewingBatchRequestCount] = useState(0);
  const [viewingBatchTimeSinceLastAction, setViewingBatchTimeSinceLastAction] = useState("");
  const [workflowStepsMap, setWorkflowStepsMap] = useState({});
  const [vidDetails, setVidDetails] = useState(null);
  const [dynamicSteps, setDynamicSteps] = useState([]);
  const [workflowHistory, setWorkflowHistory] = useState([]);
  const [isFetchingVid, setIsFetchingVid] = useState(false);
  const [isFetchingSteps, setIsFetchingSteps] = useState(false);
  const [isDescExpanded, setIsDescExpanded] = useState(false);
  const [dbRoleMap, setDbRoleMap] = useState({});
  const [viewingBatchOrder, setViewingBatchOrder] = useState(1);
  const [viewingBatchRawStatus, setViewingBatchRawStatus] = useState("");

  useEffect(() => {
    const fetchRoles = async () => {
      try {
        const rolesData = await callApi(
          "/JS/voucher/active-roles",
          null,
          "GET"
        );
        if (rolesData && isMounted.current) setDbRoleMap(rolesData);
      } catch (err) { }
    };
    fetchRoles();
  }, [callApi]);

  const getRoleDisplayName = (roleValue) => {
    if (!roleValue || roleValue === "null" || roleValue === "System")
      return "System";
    return dbRoleMap[String(roleValue)] || roleValue;
  };

  const handleDownloadExcel = async () => {
    if (!viewingBatchId) return;

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
      if (!blobData) throw new Error("No data received");

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
      showSnackBar("Failed to download Excel file.", "error");
    }
  };

// --- SAFE AUTH CHECK (Redux) ---
  useEffect(() => {
    isMounted.current = true;
    
    // Use the Redux 'user' object directly
    if (user && user.userId) {
      setCurrentUserId(String(user.userId));
    }
    
    return () => {
      isMounted.current = false;
    };
  }, [user]); // Add 'user' to dependency array

  const fetchMyBatches = useCallback(async (isSilent = false) => {
    if (!isSilent) setLoading(true);
   try {
      // Synchronously grab the role directly from the Redux user object
      const actualRoleId = user?.roleId || user?.role || user?.ROLE_ID || null;

      // --- EXTRACT SEARCH FILTERS CORRECTLY ---
      let batchIdSearch = "";
      let vidSearch = "";

      if (filterModel && filterModel.items) {
        filterModel.items.forEach(item => {
          if (item.field === 'batchId' && item.value) batchIdSearch = item.value;
          if (item.field === 'vid' && item.value) vidSearch = item.value;
        });
      }

      // --- THE FIX: We must pass batchId and vid in the API call! ---
      const apiUrl = `/JS/voucher/all-requests-summary?roleId=${actualRoleId}&page=${paginationModel.page}&size=${paginationModel.pageSize}&batchId=${encodeURIComponent(batchIdSearch)}&vid=${encodeURIComponent(vidSearch)}`;

      const response = await callApi(apiUrl, null, "GET");

      console.log("2. Raw API Response:", response);

      let fetchedRows = [];
      if (response && response.content) {
        fetchedRows = response.content;
        setTotalRowCount(response.totalElements);
      } else if (Array.isArray(response)) {
        fetchedRows = response;
        setTotalRowCount(response.length);
      }

      console.log("3. Extracted Rows:", fetchedRows);

      const uniqueWorkflows = [
        ...new Set(
          fetchedRows.map((row) => row?.approvalWorkflow).filter(Boolean)
        ),
      ];
      console.log("4. Unique Workflows to fetch steps for:", uniqueWorkflows);

      const stepsMap = {};
      await Promise.all(
        uniqueWorkflows.map(async (wfCode) => {
          try {
            const stepsResponse = await callApi(
              `/JS/voucher/workflow/steps/${wfCode}`,
              null,
              "GET"
            );
            const steps = Array.isArray(stepsResponse)
              ? stepsResponse
              : stepsResponse?.data || [];

            stepsMap[wfCode] = steps;
          } catch (e) {
            console.warn(`Failed to fetch steps for ${wfCode}`, e);
            stepsMap[wfCode] = [
              { state: "P", designation: "Creator" },
              { state: "APPROVED", designation: "Approver" }
            ];
          }
        })
      );

      console.log("5. Final Steps Map:", stepsMap);

      if (isMounted.current) {
        setWorkflowStepsMap(stepsMap);
        setRows(fetchedRows);
      }
    } catch (err) {
      console.error("CATCH BLOCK TRIGGERED. Full Error Object:", err);
      console.error("Error Message:", err.message);

      if (isMounted.current && err.name !== "CanceledError") {
        showSnackBar(
          `Failed to load: ${err.message}`,
          "error"
        );
      }
    } finally {
      if (isMounted.current) setLoading(false);
    }
    // --- THE FIX: Must add filterModel to dependencies so React re-runs this when you type ---
  }, [callApi, showSnackBar, paginationModel.page, paginationModel.pageSize, filterModel]);

  useEffect(() => {
    fetchMyBatches();
  }, [fetchMyBatches]);

  const hasProcessingBatches = rows.some((row) => row.batchStatus === 'S');

  useEffect(() => {
    let interval;
    if (hasProcessingBatches) {
      interval = setInterval(() => {
        fetchMyBatches(true); // true = silent mode
      }, 5000); // Ping server every 5 seconds
    }
    return () => clearInterval(interval);
  }, [hasProcessingBatches, fetchMyBatches]);

  const handleOpenModal = (batch) => {
    setSelectedBatch(batch);
    setIsModalOpen(true);
  };
  const handleCloseModal = () => {
    if (isSubmitting) return;
    setIsModalOpen(false);
    setSelectedBatch(null);
  };
