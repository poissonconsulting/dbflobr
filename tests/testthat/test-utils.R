context("utils")

test_that("add_blob_column works", {

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  df <- data.frame(x = 1)

  expect_true(DBI::dbCreateTable(conn, "df", df))

  ## add blob_column
  expect_error(add_blob_column(table_name = "df", column_name = "x", conn = conn),
               "column 'x' already exists")
  expect_true(add_blob_column(table_name = "df", column_name = "flob", conn = conn))

  df2 <- DBI::dbReadTable(conn, "df")
  expect_identical(colnames(df2), c(colnames(df), c("flob")))
  expect_is(df2$flob, "blob")

  result <- DBI::dbSendQuery(conn = conn, statement = paste("SELECT flob FROM df LIMIT 1"))
  expect_identical(DBI::dbColumnInfo(result)$type, "list")
  DBI::dbClearResult(result)

  ## flob
  flob <- flobr::flob_obj
  x <- collapse_flob(flob)
  expect_is(x, "character")
  expect_length(x, 1L)
})
