
  const columns = useMemo(() => {
    const cols = [
      { field: "vid", headerName: "VID", flex: 1 },
      {
        field: "categoryName",
        headerName: "Category",
        flex: 1,
      },
      {
        field: "allowedRoleName",
        headerName: "Role",
        flex: 1,
      },

      {
        field: "status",
        headerName: "Status",
        flex: 1,
      },

      {
        field: "createdAt",
        headerName: "Created At",
        flex: 1.5,
        renderCell: (params) =>
          params.value
            ? new Date(
              params.value
            ).toLocaleString()
            : "",
      },

      {
        field: "description",
        headerName: "Details",
        flex: 1,
        renderCell: (params) => (
          <Button
            variant="text"
            onClick={() =>
              handleViewDescription(
                params.row.description,
                params.row.issueCategories
              )
            }
          >
            View
          </Button>
        ),
      },
    ];

    if (searchedStatus === "TERMINATED") {
      cols.push({
        field: "remarks",
        headerName: "Remark",
        flex: 1,

        renderCell: (params) => (
          <Button
            variant="text"
            onClick={() =>
              handleViewRemark(
                params.row.remarks
              )
            }
          >
            View
          </Button>
        ),
      });
    }


    if (
      searchedStatus === "ACTIVE"

    ) {
      cols.push({
        field: "action",
        headerName: "Action",
        flex: 1,

        renderCell: (params) =>
          params.row.status ===
            "ACTIVE" ? (
            <Button
              color="error"
              variant="contained"
              startIcon={<BlockIcon />}
              onClick={() =>
                handleDeleteClick(
                  params.row
                )
              }
            >
              Terminate
            </Button>
          ) : null,
      });
    }
    return cols;
  }, [searchedStatus]);

  return (
    <Box
      sx={{
        p: 3,
        height: "calc(100vh - 88px)",
        minHeight: 0,
        maxHeight: "calc(100vh - 88px)",
        overflow: "hidden",
        boxSizing: "border-box",
      }}
    >
      <Paper
        sx={{
          p: 3,
          height: "100%",
          minHeight: 0,
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
        }}
      >
        <Typography variant="h5" mb={2}>
          View Voucher Requests
        </Typography>

        <Accordion expanded={expanded} onChange={() => setExpanded(!expanded)}>
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Typography>Search Filters</Typography>
          </AccordionSummary>

          <AccordionDetails>
            <Box
              sx={{
                display: "grid",
                gridTemplateColumns: "1.2fr 1.2fr 1.1fr 1.1fr auto auto",
                gap: 2,
              }}
            >
              <TextField
                label="VID"
                size="small"
                value={searchVID}
                error={Boolean(vidError)}
                helperText={
                  vidError
                    ? vidError
                    : "Format: VID followed by numbers (VID-2606-00102). Leave blank to search all Active vouchers."
                }
                onChange={handleVIDChange}
              />
              <LocalizationProvider dateAdapter={AdapterDayjs}>
                <DatePicker
                  disableFuture
                  label="Date"
                  value={searchDate}
                  minDate={dayjs("2026-04-01")}
                  onChange={(newValue) => setSearchDate(newValue)}
                  slotProps={{
                    textField: {
                      size: "small",
                      fullWidth: true,
                      helperText: "Select a date for search",
                      onKeyDown: (e) => e.preventDefault(),
                      inputProps: {
                        readOnly: true,
                      },
                    },
                  }}
                />
              </LocalizationProvider>

              <FormControl size="small">
                <InputLabel>Status</InputLabel>
                <Select
                  value={searchStatus}
                  label="Status"
                  sx={{
                    border: "none",
                  }}
                  onChange={(e) => setSearchStatus(e.target.value)}
                >
                  {voucherStatuses.map((item, index) => (
                    <MenuItem key={index} value={item.value}>
                      {item.value}
                    </MenuItem>
                  ))}
                </Select>
                <FormHelperText>
                  Select the voucher status to filter the records.
                </FormHelperText>
              </FormControl>

              <FormControl size="small">
                <InputLabel>Category</InputLabel>
                <Select
                  value={searchCategory}
                  label="Category"
                  onChange={(e) => setSearchCategory(e.target.value)}
                >
                  {voucherCategories.map((item) => (
                    <MenuItem key={item.id} value={item.id}>
                      {item.categoryName}
                    </MenuItem>
                  ))}
                </Select>
                <FormHelperText>
                  Select a voucher category or leave blank to search all
                  categories.
                </FormHelperText>
              </FormControl>

              <Button
                size="small"
                variant="contained"
                onClick={fetchVouchers}
                disabled={Boolean(vidError)}
                sx={{ height: 40, minWidth: 90, textTransform: "none" }}
              >
                Search
              </Button>

              <Button
                size="small"
                sx={{ height: 40, minWidth: 90, textTransform: "none",  
                  "&:hover":
                  {
                    color:"#fff",
                  },
                }}
                variant="outlined"
                onClick={handleReset}
              >
                Reset
              </Button>
            </Box>
          </AccordionDetails>
        </Accordion>

        <Box
          sx={{
            display: "flex",
            justifyContent: "flex-end",
            mt: 2,
            mb: 1,
            flexShrink: 0,
          }}
        >
          <Button
            variant="contained"
            startIcon={<FileDownloadIcon />}
            onClick={handleExport}
            sx={{ textTransform: "none" }}
          >
            Export
          </Button>
        </Box>

        <Box
          sx={{
            flex: 1,
            minHeight: 0,
            width: "100%",
          }}
        >
          <DataGrid
            disableRowSelectionOnClick
            hideFooterSelectedRowCount
            rows={rows}
            columns={columns}
            sx={styles.dataGridContainer}
            getRowId={(row) => row.id}
            loading={loading}
            pageSizeOptions={[25, 50, 100]}
            initialState={{
              pagination: {
                paginationModel: {
                  page: 0,
                  pageSize: 25,
                },
              },
            }}
          />
        </Box>
      </Paper>
      <Dialog
        open={deleteDialogOpen}
        onClose={() => setDeleteDialogOpen(false)}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle>Terminate Voucher</DialogTitle>

        <DialogContent>
          <InputLabel>Remark*</InputLabel>

          <TextField
            fullWidth
            multiline
            rows={5}
            value={deleteRemark}
            onChange={(e) => {
              const value = e.target.value;

              setRemarkTouched(true);

              if (value.startsWith(" ")) {
                setRemarkError(
                  "Remark should not start with a space or contain consecutive spaces...",
                );
                return;
              }

              if (value.includes("  ")) {
                setRemarkError("Consecutive spaces are not allowed.");
                return;
              }

              setDeleteRemark(value);

              if (value.length > 200) {
                setRemarkError("Maximum 200 characters allowed.");
              } else {
                setRemarkError("");
              }
            }}
            inputProps={{ maxLength: 200 }}
            error={Boolean(remarkError)}
            helperText={
              remarkError
                ? remarkError
                : `All characters, numbers and special characters are allowed. (${deleteRemark.length}/200)`
            }
          />

          {/* <TextField
            fullWidth
            multiline
            rows={5}
            value={deleteRemark}
            // onChange={(e) => {
            //   const value = e.target.value;
             
            //   if (value.startsWith(" ")) {

            //     setRemarkError("remark should not start with a space or contain consecutive spaces..",
            //     );
            //     return;
            //   }

            //   setDeleteRemark(value);

            //   if (value.length > 200) {

            //     setRemarkError("Maximum 200 characters allowed");

            //     setTimeout(() => {
            //       setRemarkError("")
            //     }, 1500)
            //   } else {
            //     setRemarkError("")
            //   }

            // }
            // }

            onChange={(e) => {
  const value = e.target.value;

  // First space not allowed
  if (value.startsWith(" ")) {
    setDeleteRemark(""); // state update
    setRemarkError("Remark should not start with a space.");
    return;
  }

  // Consecutive spaces
  if (value.includes("  ")) {
    setDeleteRemark(value);
    setRemarkError("Consecutive spaces are not allowed.");
    return;
  }

  // Max length
  if (value.length > 200) {
    setRemarkError("Maximum 200 characters allowed.");
    return;
  }

  setDeleteRemark(value);
  setRemarkError("");
}}
             inputProps={{ maxLength: 200 }}
           
           error={!!remarkError}
helperText={
  remarkError
    ? `${remarkError} (${deleteRemark.length}/200)`
    : `All characters, numbers and special characters are allowed. (${deleteRemark.length}/200)`
}

          /> */}
        </DialogContent>

        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)}>Cancel</Button>

          <Button
            color="error"
            variant="contained"
            disabled={!deleteRemark.trim()}
            onClick={handleTerminate}
          >
            Terminate
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={descriptionPopupOpen}
        onClose={() => setDescriptionPopupOpen(false)}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle
          sx={{
            borderBottom: "1px solid #e0e0e0",
            fontWeight: 600,
          }}
        >
          Details
        </DialogTitle>

        <DialogContent sx={{ p: 0 }}>
          {/* Description Section */}

          <Box
            sx={{
              px: 3,
              py: 2,
              borderBottom: "1px solid #e0e0e0",
            }}
          >
            <Typography
              variant="subtitle2"
              sx={{
                fontWeight: 700,
                mb: 1,
              }}
            >
              Description
            </Typography>

            <Typography
              variant="body1"
              sx={{
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                overflowWrap: "break-word",
                lineHeight: 1.8,
                color: "text.primary",
                textAlign: "justify",
              }}
            >
              {selectedDescription || "-"}
            </Typography>
          </Box>

          {/* Issues Section */}

          <Box
            sx={{
              px: 3,
              py: 2,
            }}
          >
            <Typography
              variant="subtitle2"
              sx={{
                fontWeight: 700,
                mb: 1.5,
              }}
            >
              Issues
            </Typography>

            {selectedIssues.length > 0 ? (
              <Box
                component="ul"
                sx={{
                  pl: 3,
                  m: 0,
                }}
              >
                {selectedIssues.map((issue, index) => (
                  <li key={index}>
                    <Typography>{issue}</Typography>
                  </li>
                ))}
              </Box>
            ) : (
              <Typography color="text.secondary">
                No Issues Available
              </Typography>
            )}
          </Box>
        </DialogContent>

        <DialogActions
          sx={{
            borderTop: "1px solid #e0e0e0",
            px: 3,
            py: 2,
          }}
        >
          <Button
            variant="contained"
            onClick={() => setDescriptionPopupOpen(false)}
          >
            Close
          </Button>
        </DialogActions>
      </Dialog>
      <Dialog
        open={remarkPopupOpen}
        onClose={() => setRemarkPopupOpen(false)}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle>Termination Remark</DialogTitle>

        <DialogContent>
          <Typography>{selectedRemark}</Typography>
        </DialogContent>

        <DialogActions>
          <Button onClick={() => setRemarkPopupOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={snackbar.open}
        autoHideDuration={3000}
        onClose={() =>
          setSnackbar({
            ...snackbar,
            open: false,
          })
        }
        anchorOrigin={{
          vertical: "top",
          horizontal: "center",
        }}
      >
        <Alert severity={snackbar.severity} variant="filled">
          {snackbar.message}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default ViewVoucherRequestsScreen;

import React, { useEffect, useState } from "react";
import { useSelector } from "react-redux";
import {
  Alert,
  Box,
  Paper,
  Button,
  Card,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  FormHelperText,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Snackbar,
  TextField,
  Typography,
  Checkbox,
  Autocomplete,
  Chip
} from "@mui/material";
import { findMenuById } from "../../utils/CommonUtilities";


import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import AddIcon from "@mui/icons-material/Add";

import useApi from "../../hooks/useApi";

import ViewVoucherRequestScreen from "./ViewVoucherRequestScreen";
import { useNavigate } from "react-router-dom";

const RequestVoucherScreen = () => {
  const navigate = useNavigate();

  const menus = useSelector((state) => state.menus);
  const menuItems = menus.menus;
  console.log("menu bnmnmnmkn", menuItems)
  const selectedMenuItem = menus.selectedMenuItem;
  console.log("selected", selectedMenuItem)

  const [customIssueError, setCustomIssueError] =
    useState("");

  const [showCustomIssue, setShowCustomIssue] =
    useState(false);

  const [customIssueInput, setCustomIssueInput] =
    useState("");

  const [customIssues, setCustomIssues] =
    useState([]);

  const [issueList, setIssueList] =
    useState([]);
  const [selectedIssues, setSelectedIssues] =
    useState([]);

  const [issueOpen, setIssueOpen] =
    useState(false);


  const { user } = useSelector((state) => state.auth);

  const { callApi } = useApi();
const [voucherCategories, setVoucherCategories] =
    useState([]);

  const [roles, setRoles] =
    useState([]);

  const [loading, setLoading] =
    useState(false);

  const [roleLoading, setRoleLoading] =
    useState(false);

  const [openDialog, setOpenDialog] =
    useState(false);

  const [createdVID, setCreatedVID] =
    useState("");

  const [errors, setErrors] =
    useState({});

  const [snackbar, setSnackbar] =
    useState({
      open: false,
      message: "",
      severity: "success",
    });

  const [formData, setFormData] =
    useState({
      categoryId: "",
      otherCategory: "",
      description: "",
      roleId: "",
      sdRequestNumber: "",
    });

  useEffect(() => {
    fetchRoles();
    fetchVoucherCategories();
  }, []);

  const fetchVoucherCategories =
    async () => {
      try {
        const response = await callApi(
          "/VE/voucher-transactions/voucher-categories",
          null,
          "GET"
        );

        console.log(
          "Voucher Categories Response =>",
          response
        );

        const categories =
          Array.isArray(response)
            ? response
            : response?.data || [];

        const sortedCategories = [...categories].sort((a, b) => {
          if (a.categoryName === "Other") return 1;
          if (b.categoryName === "Other") return -1;
          return 0;
        });

        setVoucherCategories(sortedCategories);
      } catch (error) {
        console.error(
          "Category API Error =>",
          error
        );

        setVoucherCategories([]);
      }
    };

  const fetchRoles = async () => {
    setRoleLoading(true);

    try {
      const response = await callApi(
        "/VE/voucher-transactions/allowed-roles?menuId=30",
        null,
        "GET"
      );

      console.log(
        "Allowed Roles Response =>",
        response
      );

      setRoles(
        Array.isArray(response.data)
          ? response.data
          : []
      );
    } catch (error) {
      console.error(
        "Failed to fetch roles",
        error
      );

      setSnackbar({
        open: true,
        message: "Failed to load roles",
        severity: "error",
      });
    } finally {
      setRoleLoading(false);
    }
  };


  const handleChange = (e) => {
    const { name, value } =
      e.target;

    if (
      name === "description" &&
      value.length > 500
    ) {
      return;
    }

    if (name === "otherCategory") {
      const regex =
        /^(?!.*\s{2,})[A-Za-z0-9 -]*$/;

      if (!regex.test(value)) {
        setErrors((prev) => ({
          ...prev,
          otherCategory: "Special Character and spaces not allowed",
        }));
        return;
      }

      if (value.length >= 21) {
        setErrors((prev) => ({
          ...prev,
          otherCategory: "Maximum 20 characters allowed",
        }));
         setTimeout(() => {
          setErrors((prev) => ({
            ...prev,
            otherCategory: "",

          }))
        }, 1000)
        return;
      }


    }


    if (name === "sdRequestNumber") {
      const regex =
        /^[0-9,]*$/;

      if (!regex.test(value)) {
        setErrors((prev) => ({
          ...prev,
          sdRequestNumber: "Character, Special Characters and space not allowed",
        }));
        setTimeout(() => {
          setErrors((prev) => ({
            ...prev,
            sdRequestNumber: "",

          }))
        }, 1000)
        return;
      }

      if (value.length >= 51) {
        setErrors((prev) => ({
          ...prev,
          sdRequestNumber: "Maximum 50 number allowed",
        }));
        setTimeout(() => {
          setErrors((prev) => ({
            ...prev,
            sdRequestNumber: "",

          }))
        }, 1000)
        return;
      }

    }

    if (name === "categoryId") {

      const selectedCategory =
        voucherCategories.find(
          (item) => item.id == value
        );
      console.log(
        "Selected Category =>",
        selectedCategory
      );

      console.log(
        "Issues =>",
        selectedCategory?.issueCategories
      );

      setIssueList(
        selectedCategory?.issueCategories || []
      );

      setSelectedIssues([]);
      setCustomIssues([]);
      setCustomIssueInput("");
      setShowCustomIssue(false);
    }

if (name === "description") {
    if (value.startsWith(" ")) {
      setErrors((prev) => ({
        ...prev,
        description: "description should not start with a space or contain consecutive spaces..",
      }));
      return;
    } else {
      setErrors((prev) => ({
        ...prev,
        description: "",
      }));
    }
  }
    setFormData((prev) => ({
      ...prev,
      [name]: value,
    }));

    setErrors((prev) => ({
      ...prev,
      [name]: "",
    }));
  };


  const validateForm = () => {
    let newErrors = {};

    if (!formData.categoryId) {
      newErrors.categoryId =
        "Voucher Category is required";
    }

    const selectedCategory =
      voucherCategories.find(
        (item) =>
          String(item.id) ===
          String(formData.categoryId)
      );

    console.log(
      "Selected Category =>",
      selectedCategory
    );

    console.log(
      "Form Category Id =>",
      formData.categoryId
    );

    console.log(
      "Entered Other Category =>",
      formData.otherCategory
    );

    if (
      selectedCategory?.categoryName
        ?.trim()
        .toLowerCase() === "other" &&
      !formData.otherCategory.trim()
    ) {
      newErrors.otherCategory =
        "Please enter other category";
    }

    if (
      selectedCategory?.categoryName
        ?.trim()
        .toLowerCase() === "other" &&
      formData.otherCategory.trim()
    ) {
      const enteredCategory =
        formData.otherCategory
          .trim()
          .toLowerCase();

      const categoryExists =
        voucherCategories.some(
          (item) => {
            const match =
              item.categoryName
                ?.trim()
                .toLowerCase() ===
              enteredCategory;

            console.log(
              "Comparing =>",
              item.categoryName,
              enteredCategory,
              match
            );

            return match;
          }
        );

      console.log(
        "Category Exists =>",
        categoryExists
      );

      if (categoryExists) {
        newErrors.otherCategory =
          "Category already exists";
      }
    }

    if (
      !formData.description.trim()
    ) {
      newErrors.description =
        "Description is required";
    }

    if (
      formData.description.length >
      500
    ) {
      newErrors.description =
        "Maximum 500 Characters Allowed";
    }

    if (!formData.roleId) {
      newErrors.roleId =
        "Allowed Role is required";
    }

    if (
      selectedIssues.length === 0 &&
      customIssues.filter(
        (item) => item?.trim()
      ).length === 0
    ) {
      newErrors.issues =
        "At least one issue is required";
    }
