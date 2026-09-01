import React, { useEffect, useState, useCallback } from "react";
import useApi from "../../hooks/useApi";
import useCustomSnackbar from "../../utils/useCustomSnackbar";
import dayjs from "dayjs";
import { Box, Paper, Typography, Stack, LinearProgress ,Chip} from "@mui/material";
import CompareArrowsIcon from "@mui/icons-material/CompareArrows";
import ManageSearchOutlinedIcon from "@mui/icons-material/ManageSearchOutlined";
import { useSelector } from "react-redux";
import { getPermissions } from "../../utils/CommonUtilities";
import BalanceDifferenceFilters from "./Components/BalanceDifferenceFilters";
import BalanceDifferenceTable from "./Components/BalanceDifferenceTable";
import { StyledButton } from "./Components/BalanceDifferenceStyles";
import downloadFile from "../../utils/DownloadUtils";

export default function BalanceRengeSearchScreen() {
  const { callApi } = useApi();
  const showSnackBar = useCustomSnackbar();
  const user = useSelector((state) => state.auth.user);
  const selectedMenu = useSelector((state) => state.menus.selectedMenuItem);
  const permissions = getPermissions(selectedMenu);
  const chipSx = {
  height: 28,
  borderRadius: 1.5,
  fontSize: "11px",
  fontWeight: 600,
  color: "#58469f",
  borderColor: "rgba(88,70,159,.28)",
  bgcolor: "rgba(88,70,159,.035)",
};

  const [data, setData] = useState(null);
  const [branches, setBranches] = useState([]);
  const [cgls, setCgls] = useState([]);
  const [currencies, setCurrencies] = useState([]);
  const [start, setStart] = useState(null);
  const [end, setEnd] = useState(null);
  const [currency, setCurrency] = useState("");
  const [cgl, setCgl] = useState(null);
   const [exportLoading,setExportLoading] =useState(false);
  const branchCodeStr = String(user?.branch || "").padStart(5, "0");
  const [branch, setBranch] = useState(
    user?.isCircle === false ? `${branchCodeStr}-${user?.branchName || ""}` : ""
  );
  const [etlDate, setEtlDate] = useState(null);
  const [searchLoading, setSearchLoading] = useState(false);
  const [glcc, setGlcc] = useState("");
  const [glccValidated, setGlccValidated] = useState(false);
  const [glccLoading, setGlccLoading] = useState(false);
  const [loading, setLoading] = useState({
    branch: false,
    cgl: false,
    currency: false,
    balance: false,
  });
  const [rowCount, setRowCount] = useState(0);
  const [req] = useState({ branch: true, cgl: true });
  const [filtersExpanded, setFiltersExpanded] = useState(true);
  const [paginationModel, setPaginationModel] = useState({
    page: 0,
    pageSize: 25,
  });

  const isNumeric = (value) => (value ? /^\d+$/.test(value) : false);

  const fetchDifferences = useCallback(async () => {
    try {
      setLoading((prev) => ({ ...prev, balance: true }));

      const matched = branch?.match(/^(\d{5})-/);
      if (!matched || !start || !end) return;

      const payload = {
        fromDate: start.format("YYYY-MM-DD"),
        toDate: end.format("YYYY-MM-DD"),
        branchCode: matched[1],
        currency: currency || null,
        cgl: cgl?.split(" - ")[0] || null,
      };

      const response = await callApi(
        `/ES/differences/search?page=${paginationModel.page}&size=${paginationModel.pageSize}`,
        payload,
        "POST"
      );

      const content = response?.data?.content ?? response?.content ?? [];
      const totalElements =
        response?.data?.page?.totalElements || 0;

      if (!Array.isArray(content)) {
        throw new Error("Unexpected response format");
      }

      const mappedData = content.map((item, index) => ({
        id: item.id ?? `difference-${paginationModel.page}-${index}`,
        errorDate: item.FIRST_ERROR_DATE || "-",
        reconDate: item.reconRunDate || "-",
        branch: item.branchCode || "-",
        branchName: item.branchName || "NA",
        currency: item.currency || "-",
        currencyName: item.currencyName || "NA",
        cgl: item.cgl || "-",
        cglDescription: item.cglDescription || item.description || "NA",
        cbsBalance: item.cbsBalance ?? 0,
        glBalance: item.glBalance ?? 0,
        differenceAmount: item.differenceAmount ?? 0,
        diffYesterday: item.diffBwYesterday ?? item.diffYesterday ?? 0,
        type: item.type || "-",
        head: item.head || "-",
      }));

      setData(mappedData);
      setRowCount(totalElements);
      setFiltersExpanded(false);

      if (mappedData.length === 0) {
        showSnackBar("No records found", "info");
      }
    } catch (error) {
      console.error("Balance Difference API Error:", error);
      setData([]);
      setRowCount(0);
      showSnackBar(
        error?.message || "Failed to fetch balance difference records.",
        "error"
      );
    } finally {
      setLoading((prev) => ({ ...prev, balance: false }));
      setSearchLoading(false);
    }
  }, [
    callApi,
    branch,
    currency,
    cgl,
    start,
    end,
    paginationModel,
    showSnackBar,
  ]);

  const validateGlcc = async (value) => {
    try {
      setGlccLoading(true);

      const response = await callApi(
        "/CM/common-master/validate-glcc",
        { glcc: value },
        "POST"
      );

      const glccData = response?.data;

      if (glccData?.valid) {
        setBranch(
          `${glccData.branchCode}-${glccData.branchName?.trim() || ""}`
        );
        setCurrency(glccData.currencyCode);
        setCgl(`${glccData.cglNumber} - ${glccData.cglDescription}`);
        setGlccValidated(true);
      } else {
        setGlccValidated(false);
        setBranch("");
        setCurrency("");
        setCgl("");
        showSnackBar(
          glccData?.errors?.join(", ") || "Invalid GLCC",
          "error"
        );
      }
    } catch (error) {
      setGlccValidated(false);
      setBranch("");
      setCurrency("");
      setCgl("");

      const errors =
        error?.response?.data?.data?.errors ||
        error?.response?.data?.errors;

      showSnackBar(errors?.join(", ") || "Invalid GLCC", "error");
    } finally {
      setGlccLoading(false);
    }
  };

  const fetchCurrencies = async () => {
    setLoading((prev) => ({ ...prev, currency: true }));

    try {
      const response = await callApi(
        "/CM/common-master/currency-code-name-only",
        null,
        "GET"
      );

      const sortedData = (response?.data || []).sort((a, b) => {
        if (a.currencyCode === "INR") return -1;
        if (b.currencyCode === "INR") return 1;
        return a.currencyName.localeCompare(b.currencyName);
      });

      setCurrencies(sortedData);
    } catch {
      showSnackBar("Currency data not available", "error");
    } finally {
      setLoading((prev) => ({ ...prev, currency: false }));
    }
  };

  const fetchSearchData = useCallback(
    async (type, term) => {
      try {
        setLoading((prev) => ({ ...prev, [type]: true }));

        let circleValue = "";

        if (permissions?.wholebank === true) {
          circleValue = "null";
        } else if (user?.isCircle === true || permissions?.circle) {
          circleValue = user?.circleCode;
        }

        const url =
          type === "branch"
            ? `/CM/common-master/branches-code-name-only?q=${encodeURIComponent(
                term
              )}&circleCode=${circleValue || ""}`
            : `/CM/common-master/cgl-code-description-only?q=${encodeURIComponent(
                term
              )}`;

        const response = await callApi(url, null, "GET");

        if (response?.data?.length > 0) {
          if (type === "branch") {
            setBranches(
              response.data.map((item) => `${item.code}-${item.name}`)
            );
          } else {
            setCgls(
              response.data.map(
                (item) => `${item.cglNumber} - ${item.description}`
              )
            );
          }
        } else {
          showSnackBar("Data not available", "error");
        }
      } catch (error) {
        console.error(error);
      } finally {
        setLoading((prev) => ({ ...prev, [type]: false }));
      }
    },
    [callApi, permissions, user, showSnackBar]
  );

  const handleSearchChange = (value, reason, type) => {
    if (reason === "input" && value?.length >= 3) {
      fetchSearchData(type, value);
    } else if (reason === "clear" || reason === "blur") {
      if (type === "branch") setBranches([]);
      if (type === "cgl") setCgls([]);
    }
  };

  const handleSubmit = () => {
    setSearchLoading(true);

    if (paginationModel.page === 0) {
      fetchDifferences();
    } else {
      setPaginationModel((prev) => ({ ...prev, page: 0 }));
    }
  };

  const handleExport = async () => {
    try {
      setExportLoading(true);
  
      const payload = {
        fromDate: start.format("YYYY-MM-DD"),
        toDate: end.format("YYYY-MM-DD"),
        branchCode: branch?.split("-")[0],
        currency:currency,
        cgl: cgl?.split(" - ")[0],
      };
  
      const downloadResponse = await callApi(
        "/ES/differences/export",
        payload,
        "POST",
        "arraybuffer",
      );
  
      const fileName = `Balance_Difference_${payload.reconRunDate}`;
  
      if (downloadResponse && downloadResponse?.byteLength > 0) {
        downloadFile(downloadResponse, "excel", fileName);
        return;
      }
    } catch (error) {
      console.error("Something Went Wrong!!", error);
      showSnackBar("Download Failed", "error");
    } finally {
      setExportLoading(false);
    }
  };

  const resetState = () => {
    setData(null);
    setCgl(null);
    setCgls([]);
    setBranch(
      user?.isCircle === false
        ? `${branchCodeStr}-${user?.branchName || ""}`
        : ""
    );
    setCurrency("");
    setStart(null);
    setEnd(null);
    setBranches([]);
    setGlcc("");
    setGlccValidated(false);
    setFiltersExpanded(true);
    setPaginationModel({ page: 0, pageSize: 25 });
  };

  useEffect(() => {
    if (data) {
      fetchDifferences();
    }
  }, [paginationModel.page, paginationModel.pageSize]);

  useEffect(() => {
    fetchCurrencies();
  }, []);

  useEffect(() => {
    const fetchSystemDate = async () => {
      try {
        const response = await callApi(
          "/PS/file/fincore-date",
          {},
          "GET"
        );
        const etlRaw = response?.data?.userDate;
        setEtlDate(
          etlRaw ? dayjs(etlRaw.split("T")[0]) : dayjs()
        );
      } catch {
        setEtlDate(dayjs());
      }
    };

    fetchSystemDate();
  }, [callApi]);

  return (
    <Box sx={{ p: 1 }}>
      <Paper
        elevation={0}
        sx={{
          p: { xs: 1.5, sm: 2, md: 2.5 },
          mt: { xs: -1, sm: -2 },
          mb: 3,
          bgcolor: "rgba(255,255,255,0.4)",
          backdropFilter: "blur(4px)",
          border: "1px solid",
          borderColor: "divider",
          borderRadius: { xs: 2, sm: 3 },
          overflow: "hidden",
        }}
      >
       <Box
  sx={{
    mb: filtersExpanded ? 1 : 0.5,
    p: { xs: 1.5, sm: 2 },
    borderRadius: 2,
    bgcolor: "background.paper",
    boxShadow: 2,
    border: "1px solid",
    borderColor: "divider",
  }}
>
  <Stack
    direction={{ xs: "column", sm: "row" }}
    alignItems={{ xs: "flex-start", sm: "center" }}
    justifyContent="space-between"
    spacing={2}
  >
    <Stack direction="row" spacing={2} alignItems="center">
      <Box
        sx={{
          width: 44,
          height: 44,
          borderRadius: 2,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          bgcolor: "rgba(88, 70, 159, 0.1)",
          border: "1px solid rgba(88, 70, 159, 0.2)",
          flexShrink: 0,
        }}
      >
        <CompareArrowsIcon
          sx={{
            fontSize: 30,
            color: "#58469f",
          }}
        />
      </Box>

      <Box>
        <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap">
          <Typography
            variant="subtitle1"
            fontWeight={700}
            lineHeight={1.2}
            sx={{
              fontSize: { xs: "0.95rem", sm: "1rem", md: "1.05rem" },
            }}
          >
            Balance Comparison
          </Typography>

          <Chip
            label="CBS ↔ GL"
            size="small"
            sx={{
              height: 22,
              fontSize: "0.68rem",
              fontWeight: 700,
              color: "#58469f",
              bgcolor: "rgba(88, 70, 159, 0.08)",
              border: "1px solid rgba(88, 70, 159, 0.2)",
            }}
          />
        </Stack>

        <Typography
          variant="body2"
          color="text.secondary"
          sx={{
            mt: 0.3,
            fontSize: { xs: "0.75rem", sm: "0.82rem" },
          }}
        >
          Compare CBS and GL balances to identify differences and discrepancies.
        </Typography>
      </Box>
    </Stack>

    
{data && (
  <StyledButton
    variant="contained"
    startIcon={<ManageSearchOutlinedIcon />}
    onClick={() => setFiltersExpanded(true)}
    sx={{
      textTransform: "none",
      borderRadius: "8px",
      fontWeight: 600,
      minWidth: { xs: "100%", sm: 140 },
      width: { xs: "100%", sm: "auto" },
      height: { xs: 38, sm: 32 },
      px: 1.5,
      fontSize: { xs: "0.8rem", sm: "0.82rem" },
    }}
  >
    Edit Filters
  </StyledButton>
)}
  </Stack>
</Box>

{data && (
  <Stack
    direction="row"
    spacing={0.8}
    flexWrap="wrap"
    useFlexGap
    sx={{
      mt: 1.5,
      px: 0.5,
    }}
  >
    <Chip
      label={`Branch: ${branch || "-"}`}
      variant="outlined"
      size="small"
      sx={chipSx}
    />

    <Chip
      label={`Currency: ${currency || "-"}`}
      variant="outlined"
      size="small"
      sx={chipSx}
    />

    <Chip
      label={`CGL: ${cgl || "-"}`}
      variant="outlined"
      size="small"
      sx={chipSx}
    />

    <Chip
      label={`Date: ${start?.format("DD MMM YYYY") || "-"} - ${
        end?.format("DD MMM YYYY") || "-"
      }`}
      variant="outlined"
      size="small"
      sx={chipSx}
    />
  </Stack>
)}

{filtersExpanded && searchLoading && !data?.length && (
          <LinearProgress
            sx={{
              height: 3,
              borderRadius: 999,
              mb: 2,
            }}
          />
        )}

        {filtersExpanded && (
          <BalanceDifferenceFilters
            branch={branch}
            setBranch={setBranch}
            currency={currency}
            setCurrency={setCurrency}
            cgl={cgl}
            setCgl={setCgl}
            start={start}
            setStart={setStart}
            end={end}
            setEnd={setEnd}
            currencies={currencies}
            branches={branches}
            cgls={cgls}
            loading={loading}
            req={req}
            handleSearchChange={handleSearchChange}
            fetchCurrencies={fetchCurrencies}
            handleSubmit={handleSubmit}
            resetState={resetState}
            isNumeric={isNumeric}
            user={user}
            permissions={permissions}
            etlDate={etlDate}
            glcc={glcc}
            setGlcc={setGlcc}
            glccValidated={glccValidated}
            setGlccValidated={setGlccValidated}
            validateGlcc={validateGlcc}
            glccLoading={glccLoading}
          />
        )}
      </Paper>

      {data && (
        <BalanceDifferenceTable
          data={data}
          loading={loading.balance}
          rowCount={rowCount}
          paginationModel={paginationModel}
          setPaginationModel={setPaginationModel}
          exportLoading={exportLoading}
          handleDownloadExcel={handleExport}
        />
      )}
    </Box>
  );
}
