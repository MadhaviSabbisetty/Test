import React from "react";

import { DataGrid } from "@mui/x-data-grid";

import {
  Button,
  Typography,
  Stack,
  Box,
  Chip,
  CircularProgress,
} from "@mui/material";

import FileDownloadIcon from "@mui/icons-material/FileDownload";
import ErrorOutlineIcon from "@mui/icons-material/ErrorOutline";

import {
  balanceDifferenceColumns,
} from "./BalanceDifferenceColumns";

import { OverlayBox } from "./BalanceDifferenceStyles";


const CustomNoRowsOverlay = () => (
  <OverlayBox>
    <ErrorOutlineIcon
      fontSize="large"
      color="action"
    />

    <Typography
      variant="h5"
      fontSize="1.2rem"
      color="text.secondary"
    >
      No difference records available for the selected criteria
    </Typography>
  </OverlayBox>
);


export default function BalanceDifferenceTable({
  data,
  loading,
  rowCount,
  paginationModel,
  setPaginationModel,
  handleDownloadExcel,
  exportLoading,
}) {
    console.log(data);
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
                rowCount > 0
                  ? `${rowCount.toLocaleString(
                      "en-IN",
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
                <FileDownloadIcon />
              )
            }
            onClick={handleDownloadExcel}
            disabled={
              !data?.length
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
            rows={data || []}
            columns={balanceDifferenceColumns}
            loading={loading}
            rowCount={rowCount}

            paginationMode="server"

            pageSizeOptions={[
              25,
              50,
              75,
              100,
            ]}

            paginationModel={
              paginationModel
            }

            onPaginationModelChange={
              setPaginationModel
            }

            slots={{
              noRowsOverlay:
                CustomNoRowsOverlay,
            }}

            rowHeight={45}

            disableRowSelectionOnClick

            sx={{
              borderRadius: "20px",
              backgroundColor:
                "rgba(255,255,255,0.4)",
              border:
                "1px solid rgba(255,255,255,0.3)",

              "& .MuiDataGrid-columnHeaders":
                {
                  bgcolor: "#f8f9fa",
                  fontWeight: 700,
                },
            }}
          />
        </Box>
      </Box>
    </Box>
  );
}
