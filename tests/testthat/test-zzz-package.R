context("package")

test_that("works with row id", {
  file <- ":memory:"

#  file <- "test.sqlite"
  if(file.exists(file)) unlink(file)

  conn <- DBI::dbConnect(RSQLite::SQLite(), file)
  on.exit(DBI::dbDisconnect(conn))

  DBI::dbGetQuery(conn, "CREATE TABLE df (
           char TEXT NOT NULL,
             num REAL NOT NULL,
             key REAL PRIMARY KEY NOT NULL)")

  df <- data.frame("char" = c("a", "b", "b"),
                   num = c(1.1, 2.2, 2.2),
                   key = c(1, 2, 3),
                   stringsAsFactors = FALSE)

  DBI::dbWriteTable(conn, name = "df", value = df, row.names = FALSE,
               append = TRUE)

  ## blob_column
  expect_error(blob_column(table_name = "df", column_name = "char", conn = conn),
               "'char' already exists")
  expect_error(blob_column(table_name = "test", column_name = "blob", conn = conn),
               "'test' is not an existing table")
  expect_true(blob_column(table_name = "df", column_name = "blob", conn = conn))
  expect_true(blob_column(table_name = "df", column_name = "flob", conn = conn))
  expect_error(blob_column(table_name = "df", column_name = "blob", conn = conn),
               "'blob' already exists")

  df2 <- DBI::dbReadTable(conn, "df")
  expect_identical(df2[c("char", "num", "key")], df)
  expect_identical(colnames(df2), c(colnames(df), c("blob", "flob")))
  expect_is(df2$blob, "blob")
  expect_is(df2$flob, "blob")

  result <- DBI::dbSendQuery(conn = conn, statement = paste("SELECT blob FROM df LIMIT 1"))
  expect_identical(DBI::dbColumnInfo(result)$type, "list")

  ### slob
  flob <- flobr::flob_obj

  expect_is(flob_collapse(flob), "character")
  expect_length(flob_collapse(flob), 1L)

  expect_error(write_flob(flob = 1, table_name = "df", column_name = "blob", conn = conn, rowid = 1L),
               "flob must inherit from class flob")
  expect_error(write_flob(flob = flob, table_name = "test", column_name = "blob", conn = conn, rowid = 1L),
               "'test' is not an existing table")
  expect_error(write_flob(flob = flob, table_name = "df", column_name = "test", conn = conn, rowid = 1L),
               "'test' is not an existing column")
  expect_error(write_flob(flob = flob, table_name = "df", column_name = "char", conn = conn, rowid = 1L),
               "'char' must be type 'list'")
  expect_error(write_flob(flob = flob, table_name = "df", column_name = "blob", conn = conn, rowid = "a"),
               "rowid must be class integer")
  expect_error(write_flob(flob = flob, table_name = "df", column_name = "blob", conn = conn, rowid = 5L),
               "rowid does not exist")

  expect_true(write_flob(flob = flob, table_name = "df", column_name = "blob", conn = conn, rowid = 1L))

  df3 <- DBI::dbReadTable(conn, "df")
  expect_identical(df3[-1,], df2[-1,])
  expect_equal(df3$blob[1], flob, check.names = FALSE, check.attributes = FALSE)

  ### unslob
  expect_error(read_flob(conn = conn, table_name = "df", column_name = "blob", rowid = 2L), "There is no flob to retrieve")
  expect_error(read_flob(conn = conn, table_name = "test", column_name = "blob", rowid = 1L), "'test' is not an existing table")
  expect_error(read_flob(conn = conn, table_name = "df", column_name = "blo", rowid = 1L), "'blo' is not an existing column")
  expect_error(read_flob(conn = conn, table_name = "df", column_name = "blob", rowid = 4L), "rowid does not exist")

  flob2 <- read_flob(conn, table_name = "df", column_name = "blob", rowid = 1L)
  expect_equal(flob2, flob, check.names = FALSE)
})

test_that("no row id", {
  file <- ":memory:"
#  file <- "norowid.sqlite"
  if(file.exists(file)) unlink(file)

  conn <- DBI::dbConnect(RSQLite::SQLite(), file)
  on.exit(DBI::dbDisconnect(conn))

  DBI::dbGetQuery(conn, "CREATE TABLE df (
                  char TEXT NOT NULL,
                  num REAL NOT NULL,
                  key REAL PRIMARY KEY NOT NULL) WITHOUT ROWID")

  df <- data.frame("char" = c("a", "b", "b"),
                   num = c(1.1, 2.2, 2.2),
                   key = c(1, 2, 3),
                   stringsAsFactors = FALSE)

  DBI::dbWriteTable(conn, name = "df", value = df, row.names = FALSE,
                    append = TRUE)

  ### blob_column
  expect_true(blob_column(table_name = "df", column_name = "blob", conn = conn))

  expect_error(write_flob(flob = flobr::flob_obj, table_name = "df", column_name = "blob", conn = conn, rowid = 1L),
               "rowid column does not exist")

  expect_error(read_flob(conn = conn, table_name = "df", column_name = "blob", rowid = 1L), "rowid column does not exist")
})
