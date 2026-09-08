import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  Box,
  Button,
  Dialog,
  DialogContent,
  DialogTitle,
  Fab,
  Grid,
  IconButton,
  Stack,
  TextField,
  Typography,
  Tooltip,
  CircularProgress,
  DialogActions,
  Divider,
  styled,
} from "@mui/material";
import { DataGrid } from "@mui/x-data-grid";
import {
  Edit as EditIcon,
  Add as AddIcon,
  Close as CloseIcon,
} from "@mui/icons-material";
import useApi from "../../hooks/useApi";
import useCustomSnackbar from "../../utils/useCustomSnackbar";
import { CurrencyMasterStyles } from "./CurrencyMasterStyles";
import { currencyFormatterWith6Decimals } from "../../utils/NumberFormat";
import ErrorOutlineIcon from "@mui/icons-material/ErrorOutline";

const OverlayBox = styled(Box)(() => ({
  display: "flex",
  flexDirection: "column",
  height: "100%",
  justifyContent: "center",
  alignItems: "center",
}));

const CustomNoRowsOverlay = () => {
  return (
    <OverlayBox>
      <ErrorOutlineIcon fontSize="large" />
      <Typography variant="h5" fontSize="1.2rem">
        Data is not available
      </Typography>
    </OverlayBox>
  );
};

export default function CurrencyMasterTab() {
  const [rows, setRows] = useState([]);
  const [open, setOpen] = useState(false);
  const [editingId, setEditingId] = useState(null);
  const [loading, setLoading] = useState(true);
  const [originalData, setOriginalData] = useState(null);
  const snackbar = useCustomSnackbar();
  const { callApi } = useApi();

  const [currencyData, setCurrencyData] = useState({
    currencyCode: "",
    currencyName: "",
    currencyRate: "",
  });
  const [errors, setErrors] = useState({});

  const reDigits = /^\d{0,6}(\.\d{0,6})?$/;
  const reDesc = /^[A-Za-z\s]*$/;

  const snackGateRef = useRef(true);
  const notifyOnce = (msg, variant = "warning") => {
    if (!snackGateRef.current) return;
    snackGateRef.current = false;
    snackbar(msg, variant);
    setTimeout(() => (snackGateRef.current = true), 900);
  };

  // --- Fetch Data ---
  const fetchData = async () => {
    try {
      setLoading(true);
      const currenciesResponse = await callApi(
        "/CM/common-master/currency-master",
        null,
        "GET",
      );
      setRows(currenciesResponse?.data ?? []);
    } catch (err) {
      snackbar("Failed to fetch data", "error");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const validateField = (name, value) => {
    switch (name) {
      case "currencyCode":
        if (!value.trim()) return "Please provide the required currency code.";
        if (!reDesc.test(value)) return "Only letters are allowed.";
        if (value.length !== 3)
          return "Must be exactly 3 uppercase letters for currency code.";
        break;

      case "currencyName":
        if (!value.trim())
          return "Please provide the required currency name to proceed.";
        if (!reDesc.test(value))
          return "Invalid characters (letters and spaces only).";
        if (value.trim().length < 4)
          return "Currency Name must be atleast 4 characters.";
        if (value.length > 30)
          return "Currency Name must not exceed more than 20 characters.";
        break;

      case "currencyRate":
        if (!value.trim()) return "Please provide the required currency rate.";
        if (!reDigits.test(value))
          return "Invalid format. Max 6 digits before decimal, up to 6 after (e.g., 123456.123456).";
        if (value.endsWith(".")) {
          return "Invalid currency rate format.";
        }
        if (Number(value) === 0) {
          return "Currency Rate cannot be zero.";
        }
        break;

      default:
        return "";
    }
    return "";
  };

  const isFormValid = () => {
    let tempErrors = {};
    tempErrors.currencyCode = editingId
      ? ""
      : validateField("currencyCode", currencyData.currencyCode);
    tempErrors.currencyName = validateField(
      "currencyName",
      currencyData.currencyName,
    );
    tempErrors.currencyRate = editingId
      ? ""
      : validateField("currencyRate", currencyData.currencyRate);

    return Object.values(tempErrors).every((v) => !v);
  };

  const checkDataEquality = (current, original) => {
    if (!original) return true;
    return current.currencyName === original.currencyName;
  };

  const handleChange = (e) => {
    let { name, value } = e.target;
    let errorMsg = "";

    if (name === "currencyCode") {
      const upperValue = value.toUpperCase();

      if (/[^A-Z]/.test(upperValue)) {
        setErrors((prev) => ({
          ...prev,
          currencyCode: "Special Characters and Numericals are not allowed.",
        }));
        return;
      }
      value = upperValue.slice(0, 3);
      setErrors((prev) => ({
        ...prev,
        currencyCode:
          value.length < 3
            ? "Currency Code Must be 3 uppercase letters (e.g.,USD,INR)."
            : "",
      }));
    }

    if (name === "currencyName") {
      if (value !== "" && !/^[A-Za-z ]*$/.test(value)) {
        setErrors((prev) => ({
          ...prev,
          currencyName: "Special Characters and Numericals are not allowed.",
        }));
        return;
      }
      value = value.slice(0, 30);
    }

    if (name === "currencyRate") {
      if (value !== "" && !/^[0-9.]*$/.test(value)) {
        setErrors((prev) => ({
          ...prev,
          currencyRate: "Special Characters and Alphabeticals are not allowed.",
        }));
        return;
      }
      if (!/^\d{0,6}(\.\d{0,6})?$/.test(value)) {
        return;
      }
    }

    setCurrencyData((prev) => ({ ...prev, [name]: value }));

    errorMsg = validateField(name, value);
    setErrors((prev) => ({ ...prev, [name]: errorMsg }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();

    const allErrors = {
      currencyCode: editingId
        ? ""
        : validateField("currencyCode", currencyData.currencyCode),
      currencyName: validateField("currencyName", currencyData.currencyName),
      currencyRate: editingId
        ? ""
        : validateField("currencyRate", currencyData.currencyRate),
    };
    setErrors(allErrors);

    const valid = Object.values(allErrors).every((v) => !v);
    if (!valid) {
      notifyOnce("Please fix validation errors.", "error");
      return;
    }

    const isEditing = !!editingId;
    if (isEditing && checkDataEquality(currencyData, originalData)) {
      snackbar("No Changes Detected!", "warning");
      return;
    }

    if (!isEditing) {
      const currencyExists = rows.some(
        (row) =>
          row.currencyCode.toUpperCase() ===
          currencyData.currencyCode.toUpperCase(),
      );
      if (currencyExists) {
        snackbar("Currency code already exists.", "warning");
        return;
      }
    }

    let payloadData;
    if (isEditing) {
      payloadData = {
        requestType: "CURRENCY",
        changeType: "UPDATE",
        payload: {
          currencyCode: currencyData.currencyCode,
          currencyName: currencyData.currencyName,
          flag: 1,
          currencyRate: originalData.currencyRate,
        },
      };
    } else {
      payloadData = {
        requestType: "CURRENCY",
        changeType: "ADD",
        payload: {
          currencyCode: currencyData.currencyCode,
          currencyName: currencyData.currencyName,
          flag: 0,
          currencyRate: parseFloat(currencyData.currencyRate) || 0,
        },
      };
    }

    try {
      const response = await callApi("/CR/create-request", payloadData, "POST");
      if (response) {
        snackbar(
          editingId
            ? "Currency update request submitted successfully with request id " +
                response?.data?.id
            : "Currency creation request submitted successfully with request id " +
                response?.data?.id,
          "success",
        );
        handleClose();
        fetchData();
      }
    } catch (err) {
      snackbar(err?.message || "Request failed.", "error");
    }
  };

  const handleEdit = (row) => {
    const initialData = {
      currencyCode: String(row.currencyCode ?? ""),
      currencyName: String(row.currencyName ?? ""),
      currencyRate: String(row.currencyRate ?? ""),
    };
    setCurrencyData(initialData);
    setOriginalData(initialData);
    setEditingId(row.currencyCode);
    setOpen(true);
    setErrors({});
  };

  const handleClose = () => {
    setOpen(false);
    setCurrencyData({
      currencyCode: "",
      currencyName: "",
      currencyRate: "",
    });
    setOriginalData(null);
    setErrors({});
    setEditingId(null);
  };

  const handleBlur = (e) => {
    const { name } = e.target;

    setErrors((prev) => ({
      ...prev,
      [name]: "",
    }));
  };

  const columns = useMemo(
    () => [
      {
        field: "currencyCode",
        headerName: "Currency Code",
        flex: 1.2,
        minWidth: 200,
        sortable: true,
      },
      {
        field: "currencyName",
        headerName: "Currency Name",
        flex: 1,
        minWidth: 200,
        sortable: true,
      },
      {
        field: "currencyRate",
        headerName: "Currency Rate",
        flex: 1,
        minWidth: 150,
        align: "right",
        headerAlign: "right",
        sortable: true,
        renderCell: (params) =>
          currencyFormatterWith6Decimals("INR").format(params.value),
      },
      {
        field: "actions",
        headerName: "Actions",
        flex: 0.8,
        minWidth: 150,
        align: "center",
        headerAlign: "center",

        filterable: false,
        sortable: false,
        disableColumnMenu: true,
        renderCell: (params) => (
          <Box
            sx={{
              width: "100%",
              height: "100%",
              display: "flex",
              justifyContent: "center",
              alignItems: "center",
            }}
          >
            <Tooltip title="Edit">
              <IconButton
                // color="primary"
                size="small"
                onClick={() => handleEdit(params.row)}
              >
                <EditIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Box>
        ),
      },
    ],
    [],
  );

  return (
    <Box>
      <DataGrid
        loading={loading}
        rows={rows}
        columns={columns}
        disableRowSelectionOnClick
        getRowId={(row) => row.currencyCode}
        pageSizeOptions={[5, 10, 25, 50]}
        initialState={{
          pagination: { paginationModel: { pageSize: 10, page: 0 } },
          sorting: { sortModel: [{ field: "currencyCode", sort: "asc" }] },
        }}
        // sx={CurrencyMasterStyles.dataGridContainer}
         sx={{
          minHeight: 520,
          width: "100%",
         "& .MuiDataGrid-columnHeaders": {
          backgroundColor: "#f5f5f5 !important",
          fontWeight: 700,
          borderBottom: "2px solid rgba(88, 70, 159, 0.2)",
          position: 'sticky',
          top: 0,
          zIndex: 1,
        },
        }}
        localeText={{
          noRowsLabel: "No Currency Records Found",
        }}
        slots={{
          noRowsOverlay: CustomNoRowsOverlay,
        }}
      />

      {/* Floating Create Button */}
      <Fab
        onClick={() => {
          setEditingId(null);
          setCurrencyData({
            currencyCode: "",
            currencyName: "",
            currencyRate: "",
          });
          setErrors({});
          setOpen(true);
        }}
        color="primary"
        variant="extended"
        sx={CurrencyMasterStyles.fabButton}
      >
        <AddIcon sx={CurrencyMasterStyles.fabIcon} />
        Create
      </Fab>

      <Dialog open={open} maxWidth="sm" fullWidth>
        <Box component="form" onSubmit={handleSubmit}>
          <DialogTitle>
            <Stack
              direction="row"
              justifyContent="space-between"
              alignItems="center"
              sx={CurrencyMasterStyles.dialogTitleStack}
            >
              <Typography variant="h6" color="primary">
                {editingId ? "Edit Currency" : "Add New Currency"}
              </Typography>
              <IconButton onClick={handleClose}>
                <CloseIcon />
              </IconButton>
            </Stack>
          </DialogTitle>

          <Divider />

          <DialogContent>
            <Grid
              container
              spacing={3}
              sx={CurrencyMasterStyles.dialogContentGrid}
            >
              {/* Currency Code */}
              <Grid size={{ xs: 12 }}>
                <TextField
                  fullWidth
                  required
                  label="Currency Code"
                  name="currencyCode"
                  variant="outlined"
                  value={currencyData.currencyCode}
                  onChange={handleChange}
                  onBlur={handleBlur}
                  disabled={!!editingId}
                  error={!!errors.currencyCode}
                  helperText={
                    editingId
                      ? ""
                      : errors.currencyCode ||
                        "Must be 3 uppercase letters (e.g.,USD,INR)."
                  }
                  inputProps={{
                    maxLength: 3,
                    style: { textTransform: "uppercase" },
                  }}
                />
              </Grid>

              {/* Currency Name */}
              <Grid size={{ xs: 12 }}>
                <TextField
                  fullWidth
                  required
                  label="Currency Name"
                  name="currencyName"
                  variant="outlined"
                  value={currencyData.currencyName}
                  onChange={handleChange}
                  onBlur={handleBlur}
                  error={!!errors.currencyName}
                  helperText={
                    errors.currencyName ||
                    "Min 4 and Max 30 characters (letters and spaces only)."
                  }
                  sx={CurrencyMasterStyles.textField}
                  inputProps={{ maxLength: 30 }}
                />
              </Grid>

              {/* Currency Rate */}
              <Grid size={{ xs: 12 }}>
                <TextField
                  fullWidth
                  required
                  name="currencyRate"
                  label="Currency Rate"
                  variant="outlined"
                  value={currencyData.currencyRate}
                  onPaste={(e) => e.preventDefault()} // This works directly on the TextField
                  onChange={handleChange}
                  onBlur={handleBlur}
                  error={!!errors.currencyRate}
                  disabled={!!editingId}
                  helperText={
                    editingId
                      ? ""
                      : errors.currencyRate ||
                        "Max 6 digits before decimal, up to 6 after (e.g., 123456.123456) and paste is not allowed."
                  }
                  inputProps={{ maxLength: 13, inputMode: "decimal" }}
                />
              </Grid>
            </Grid>
          </DialogContent>

          <Divider />

          <DialogActions sx={CurrencyMasterStyles.dialogActions}>
            <Button onClick={handleClose}>Cancel</Button>
            <Button
              type="submit"
              variant="contained"
              startIcon={editingId ? <EditIcon /> : <AddIcon />}
              disabled={
                !isFormValid() ||
                (editingId && checkDataEquality(currencyData, originalData))
              }
            >
              {editingId ? "Update" : "Add"}
            </Button>
          </DialogActions>
        </Box>
      </Dialog>
    </Box>
  );
}
export const CurrencyMasterStyles = {
  tabBox: {
    borderColor: "divider",
  },
  tabs: {
    width: "100%",
    height: "100%",
  },
  customTabPanelBox: {
    py: 2,
  },

  dataGridContainer: {
    border: "none",
  },
  loadingStack: {
    height: "60vh",
  },

  fabButton: (theme) => ({
    position: "fixed",
    borderRadius: "8px",
    fontSize: "15px",
    bottom: theme.spacing(8),
    right: theme.spacing(4),
    zIndex: 1200,
  }),
  fabIcon: {
    // mr: 1,
  },

  dialogTitleStack: {
    paddingBottom: 1,
  },
  dialogActions: {
    padding: 2,
    display: "flex",
    gap: "12px",
  },

  requestDateChip: {
    color: "text.secondary",
  },

  requestStatusBox: (color) => ({
    color: `${color}.main`,
  }),

  requestStatusChip: (color) => ({
    p: 0.5,
    m: 0,
    width: "90%",
    color: `${color}.dark`,
    "& .MuiChip-icon": {
      color: `${color}.dark`,
    },
  }),

  // Fixed camelCase properties here
  textField: {
    width: "100%",
    "&.MuiInputBase-root": {
      height: "50px !important",
      borderRadius: "10px", // Added px
      alignItems: "center", // Fixed camelCase
    },
    "&.MuiFormHelperText-root": {
      minHeight: "20px", // Fixed camelCase
      lineHeight: "18px", // Fixed camelCase
      marginLeft: 0, // Fixed camelCase
      marginTop: "4px",
    },
  },
};
import * as React from "react";
import PropTypes from "prop-types";
import Tab from "@mui/material/Tab";
import Box from "@mui/material/Box";
import CurrencyRupeeIcon from "@mui/icons-material/CurrencyRupee";
import PendingActionsIcon from "@mui/icons-material/PendingActions";
import MyCurrencyRequestsTab from "./MyCurrencyRequestsTab";
import CurrencyMasterTab from "./CurrencyMasterTab";
import { Tabs, Paper } from "@mui/material";
import { CurrencyMasterStyles } from "./CurrencyMasterStyles";

function CustomTabPanel(props) {
  const { children, value, index, ...other } = props;

  return (
    <Box
      role="tabpanel"
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={CurrencyMasterStyles.customTabPanelBox}>{children}</Box>
      )}
    </Box>
  );
}

CustomTabPanel.propTypes = {
  children: PropTypes.node,
  index: PropTypes.number.isRequired,
  value: PropTypes.number.isRequired,
};

function a11yProps(index) {
  return {
    id: `simple-tab-${index}`,
    "aria-controls": `simple-tabpanel-${index}`,
  };
}

export default function CurrencyMaster() {
  const [value, setValue] = React.useState(0);

  const handleChange = (event, newValue) => {
    setValue(newValue);
  };

  return (
    <Paper
    elevation={2}
    sx={{
      display:"flex",
      flexDirection:"column",
      borderRadius:2,
      overflow:"hidden",
      minHeight:"calc(100vh - 210px)",
      width:"100%",
    }}>
    <Box>
      <Box sx={CurrencyMasterStyles.tabBox}>
        <Tabs
          sx={CurrencyMasterStyles.tabs}
          value={value}
          onChange={handleChange}
          aria-label="currency master tabs"
          variant="fullWidth"
        >
          <Tab
            label="Manage Currency"
            icon={<CurrencyRupeeIcon />}
            iconPosition="start"
            {...a11yProps(0)}
          />
          <Tab
            icon={<PendingActionsIcon />}
            iconPosition="start"
            label="My Requests"
            {...a11yProps(1)}
          />
        </Tabs>
      </Box>
      <CustomTabPanel value={value} index={0}>
        <CurrencyMasterTab />
      </CustomTabPanel>
      <CustomTabPanel value={value} index={1}>
        <MyCurrencyRequestsTab permissions={{view: true , cancel:true}}/>
      </CustomTabPanel>
    </Box>
    </Paper>
  );
}
