<DataGrid
  rows={rows}
  columns={columns}
  getRowId={(row) => row.code}
  loading={loading}
  paginationMode="server"
  sortingMode="server"
  filterMode="server"
  rowCount={rowCount}
  paginationModel={paginationModel}
  onPaginationModelChange={setPaginationModel}
  sortModel={sortModel}
  onSortModelChange={setSortModel}
  filterModel={filterModel}
  onFilterModelChange={setFilterModel}
  pageSizeOptions={[10, 20, 50, 100]}
  disableRowSelectionOnClick
  slots={{ noRowsOverlay: CustomNoRowsOverlay }}
/>
