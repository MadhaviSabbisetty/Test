import React from "react";
import {
  Box,
  Typography,
  Chip,
  Button,
  CircularProgress,
  Stack,
} from "@mui/material";
import { DataGrid } from "@mui/x-data-grid";
import FileDownloadOutlinedIcon from "@mui/icons-material/FileDownloadOutlined";
import ErrorOutlineOutlinedIcon from "@mui/icons-material/ErrorOutlineOutlined";
import dayjs from "dayjs";
import CustomChip from "../../../utils/CustomChip";

const formatAmount = (value) =>
  value != null && value !== ""
    ? Number(value).toLocaleString("en-IN", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })
    : "-";

const formatDate = (value) => {
  if (!value) return "-";

  const date = dayjs(value);

  return date.isValid() ? date.format("DD-MM-YYYY") : "-";
};

const columns = [
  {
    field: "errorDate",
    headerName: "Date",
    flex: 0.8,
    minWidth: 125,
    sortable: true,
    filterable: true,
  },

  {
    field: "branchCode",
    headerName: "Branch",
    flex: 0.8,
    minWidth: 110,
    sortable: true,
    filterable: true,
  },

  {
    field: "currency",
    headerName: "Currency",
    flex: 0.7,
    minWidth: 100,
    sortable: true,
    filterable: true,
  },

  {
    field: "cgl",
    headerName: "CGL",
    flex: 1,
    minWidth: 150,
    sortable: true,
    filterable: true,
  },

  {
    field: "cbsBalance",
    headerName: "CBS Balance",
    flex: 1,
    minWidth: 180,
    headerAlign: "right",
    align: "right",
    sortable: false,
    filterable: true,

    renderCell: (params) => {
      const value = Number(params.value);
      const negative = value < 0;

      return (
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-end",
            width: "100%",
            height: "100%",
            fontSize: "0.8rem",
          }}
        >
          <Typography
            sx={{
              fontSize: "0.8rem",
              fontWeight: 600,
              color: negative ? "#d32f2f" : "#2e7d32",
              mr: 1,
            }}
          >
            {formatAmount(value)}
          </Typography>

          <CustomChip
            label={negative ? "Dr" : "Cr"}
            size="small"
            sx={{
              height: 20,
              fontSize: "0.68rem",
              fontWeight: 700,
              bgcolor: negative
                ? "rgba(211,47,47,.08)"
                : "rgba(46,125,50,.08)",
              color: negative ? "#d32f2f" : "#2e7d32",
              border: `1px solid ${
                negative
                  ? "rgba(211,47,47,.2)"
                  : "rgba(46,125,50,.2)"
              }`,
            }}
          />
        </Box>
      );
    },
  },

  {
    field: "glBalance",
    headerName: "GL Balance",
    flex: 1,
    minWidth: 180,
    headerAlign: "right",
    align: "right",
    sortable: false,
    filterable: true,

    renderCell: (params) => {
      const value = Number(params.value);
      const negative = value < 0;

      return (
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-end",
            width: "100%",
            height: "100%",
          }}
        >
          <Typography
            sx={{
              fontSize: "0.8rem",
              fontWeight: 600,
              color: negative ? "#d32f2f" : "#2e7d32",
              mr: 1,
            }}
          >
            {formatAmount(value)}
          </Typography>

          <CustomChip
            label={negative ? "Dr" : "Cr"}
            size="small"
            sx={{
              height: 20,
              fontSize: "0.68rem",
              fontWeight: 700,
              bgcolor: negative
                ? "rgba(211,47,47,.08)"
                : "rgba(46,125,50,.08)",
              color: negative ? "#d32f2f" : "#2e7d32",
              border: `1px solid ${
                negative
                  ? "rgba(211,47,47,.2)"
                  : "rgba(46,125,50,.2)"
              }`,
            }}
          />
        </Box>
      );
    },
  },

  {
    field: "differenceAmount",
    headerName: "Difference Amount",
    flex: 1,
    minWidth: 190,
    headerAlign: "right",
    align: "right",
    sortable: false,
    filterable: true,

    renderCell: (params) => {
      const value = Number(params.value);

      return (
        <Typography
          sx={{
            width: "100%",
            height: "100%",
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-end",
            textAlign: "right",
            fontWeight: 600,
            fontSize: "0.8rem",
            color:
              value < 0
                ? "#d32f2f"
                : value > 0
                ? "#2e7d32"
                : "text.secondary",
          }}
        >
          {formatAmount(value)}
        </Typography>
      );
    },
  },

  {
    field: "diffYesterday",
    headerName: "Diff. Yesterday",
    flex: 1,
    minWidth: 170,
    headerAlign: "right",
    align: "right",
    sortable: false,
    filterable: true,

    renderCell: (params) => {
      const value = Number(params.value);

      return (
        <Typography
          sx={{
            width: "100%",
            height: "100%",
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-end",
            textAlign: "right",
            fontWeight: 600,
            fontSize: "0.8rem",
            color:
              value < 0
                ? "#d32f2f"
                : value > 0
                ? "#2e7d32"
                : "text.secondary",
          }}
        >
          {formatAmount(value)}
        </Typography>
      );
    },
  },

  {
    field: "type",
    headerName: "Type",
    flex: 0.7,
    minWidth: 100,
    sortable: false,
    filterable: true,
  },

  {
    field: "head",
    headerName: "Head",
    flex: 0.8,
    minWidth: 120,
    sortable: false,
    filterable: true,
  },

  {
    field: "firstErrorDate",
    headerName: "Error Date",
    flex: 0.8,
    minWidth: 125,
    sortable: true,
    filterable: true,

    renderCell: (params) => formatDate(params.value),
  },
];

const CustomNoRowsOverlay = () => (
  <Box
    sx={{
      display: "flex",
      flexDirection: "column",
      height: "100%",
      justifyContent: "center",
      alignItems: "center",
      gap: 1,
    }}
  >
    <ErrorOutlineOutlinedIcon
      fontSize="large"
      color="action"
    />

    <Typography
      sx={{
        fontSize: "1.2rem",
        color: "text.secondary",
        fontWeight: 500,
      }}
    >
      No difference records available for the selected criteria
    </Typography>
  </Box>
);

export default function BalanceDateWiseTable({
  rows = [],
  loading = false,
  totalElements = 0,
  paginationModel,
  setPaginationModel,
  onExport,
  exportLoading,
}) {
  const data = rows.map((r, i) => ({
    ...r,

    id: r.id || `row-${i}`,

    errorDate: formatDate(r.reconRunDate),

    branchCode:
      r.branchCode ||
      r.branch ||
      "-",

    currency:
      r.currency ||
      "-",

    cgl:
      r.cgl ||
      r.cglNumber ||
      "-",

    cbsBalance:
      r.cbsBalance ??
      0,

    glBalance:
      r.glBalance ??
      0,

    differenceAmount:
      r.differenceAmount ??
      r.difference ??
      0,

    diffYesterday:
      r.diffBwYesterday ??
      r.diffYesterday ??
      0,

    type:
      r.type ||
      "-",

    head:
      r.head ||
      "-",

    firstErrorDate:
      r.FIRST_ERROR_DATE ||
      r.errorDate ||
      "-",
  }));

  return (
    <Box
      sx={{
        width: "100%",
        height: "100%",
      }}
    >
      <Box
        sx={{
          p: 2,
          borderRadius: "20px",
          background:
            "rgba(255, 255, 255, 0.4)",
          backdropFilter: "blur(12px)",
          border:
            "1px solid rgba(255, 255, 255, 0.3)",
          boxShadow:
            "0 8px 32px rgba(0,0,0,0.05)",
          display: "flex",
          flexDirection: "column",
          gap: 2.5,
        }}
      >
        {/* HEADER */}

        <Stack
          direction={{
            xs: "column",
            sm: "row",
          }}
          spacing={2}
          justifyContent="space-between"
          alignItems={{
            xs: "stretch",
            sm: "center",
          }}
        >
          <Box
            sx={{
              display: "flex",
              flexDirection: {
                xs: "column",
                sm: "row",
              },
              alignItems: {
                xs: "flex-start",
                sm: "baseline",
              },
              gap: {
                xs: 1,
                sm: 1.5,
              },
            }}
          >
            <Typography
              variant="h6"
              fontWeight={800}
              sx={{
                color: "#1a1a1a",
                letterSpacing: "-0.5px",
              }}
            >
              Balance Difference Records
            </Typography>

            <Chip
              label={
                totalElements > 0
                  ? `${totalElements.toLocaleString(
                      "en-IN"
                    )} Records Found`
                  : "No Records"
              }
              size="small"
              sx={{
                fontWeight: 700,
                color: "#58469f",
                bgcolor:
                  "rgba(88, 70, 159, 0.1)",
                borderRadius: "8px",
                width: {
                  xs: "fit-content",
                  sm: "auto",
                },
              }}
            />
          </Box>

          <Button
            variant="contained"
            size="small"
            startIcon={
              exportLoading ? (
                <CircularProgress
                  size={16}
                  color="inherit"
                />
              ) : (
                <FileDownloadOutlinedIcon />
              )
            }
            onClick={onExport}
            disabled={
              !data?.length ||
              exportLoading
            }
            sx={{
              textTransform: "none",
              bgcolor: "#58469f",
              fontWeight: 700,
              borderRadius: "8px",
              px: 3,

              "&:hover": {
                bgcolor: "#45357a",
              },
            }}
          >
            {exportLoading
              ? "Preparing..."
              : "Export"}
          </Button>
        </Stack>

        {/* DATA GRID */}

        <Box
          sx={{
            width: "100%",
            overflowX: "auto",
            height: {
              xs: "55vh",
              sm: "60vh",
              md: 470,
            },
          }}
        >
          <DataGrid
            rows={data}
            columns={columns}
            loading={loading}
            rowCount={totalElements}
            paginationMode="server"
            paginationModel={paginationModel}
            onPaginationModelChange={
              setPaginationModel
            }
            pageSizeOptions={[
              25,
              50,
              75,
              100,
            ]}
            disableRowSelectionOnClick
            hideFooterSelectedRowCount
            rowHeight={45}
            columnHeaderHeight={52}
            slots={{
              noRowsOverlay:
                CustomNoRowsOverlay,
            }}
            sx={{
              borderRadius: "20px",
              backgroundColor:
                "rgba(255,255,255,0.4)",
              border:
                "1px solid rgba(255,255,255,0.3)",

              /* COLUMN HEADER */

              "& .MuiDataGrid-columnHeaders":
                {
                  bgcolor: "#f8f9fa",
                  fontWeight: 700,
                  borderBottom:
                    "1px solid #e0e0e0",
                },

              "& .MuiDataGrid-columnHeader":
                {
                  px: 1.5,
                  outline: "none",
                },

              "& .MuiDataGrid-columnHeaderTitle":
                {
                  fontSize: "0.8rem",
                  fontWeight: 700,
                  color: "#58469f",
                },

              /* COLUMN MENU */

              "& .MuiDataGrid-menuIconButton":
                {
                  opacity: 1,
                  visibility: "visible",
                  width: 28,
                  height: 28,
                },

              /* TABLE CELLS */

              "& .MuiDataGrid-cell":
                {
                  px: 1.5,
                  fontSize: "0.8rem",
                  color: "text.primary",
                  borderBottom:
                    "1px solid rgba(0,0,0,.06)",
                  outline: "none",
                },

              /* ROW HOVER */

              "& .MuiDataGrid-row:hover":
                {
                  bgcolor:
                    "rgba(88,70,159,.025)",
                },

              /* NO ROWS */

              "& .MuiDataGrid-overlay":
                {
                  bgcolor: "transparent",
                },

              /* FOOTER */

              "& .MuiDataGrid-footerContainer":
                {
                  height: 52,
                  minHeight: 52,
                  borderTop:
                    "1px solid #e0e0e0",
                  bgcolor:
                    "background.paper",
                },

              "& .MuiTablePagination-root":
                {
                  fontSize: "0.8rem",
                },

              "& .MuiTablePagination-selectLabel":
                {
                  fontSize: "0.8rem",
                },

              "& .MuiTablePagination-displayedRows":
                {
                  fontSize: "0.8rem",
                },
            }}
          />
        </Box>
      </Box>
    </Box>
  );
}
