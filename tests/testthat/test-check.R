context("check")

test_that("check_sqlite_connection", {
  expect_error(check_sqlite_connection(1),
               "1 must inherit from class SQLiteConnection")
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

  expect_identical(check_sqlite_connection(conn), conn)
  expect_identical(check_sqlite_connection(conn, connected = TRUE), conn)
  expect_error(check_sqlite_connection(conn, connected = FALSE),
               "conn must be disconnected")
  DBI::dbDisconnect(conn)

  expect_identical(check_sqlite_connection(conn), conn)
  expect_error(check_sqlite_connection(conn, connected = TRUE),
               "conn must be connected")
  expect_identical(check_sqlite_connection(conn, connected = FALSE), conn)
})

test_that("check_table_name", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  local <- data.frame(x = 1:2)
  expect_true(DBI::dbCreateTable(conn, "local", local))

  expect_error(check_table_name(1, conn),
               "table_name must be class character")
  expect_error(check_table_name("e", conn),
               "table 'e' does not exist")
  expect_identical(check_table_name("local", conn), "local")
})

test_that("check_column_name", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  local <- data.frame(test = 1:2)
  expect_true(DBI::dbCreateTable(conn, "local", local))

  expect_error(check_column_name("test", table_name = 1, exists = TRUE, conn),
               "table_name must be class character")
  expect_error(check_column_name("test", table_name = "e", exists = TRUE, conn),
               "no such table: e")
  expect_error(check_column_name(1, table_name = "local", exists = TRUE, conn),
               "column_name must be class character")
  expect_error(check_column_name("e", table_name = "local", exists = TRUE, conn),
               "column 'e' does not exist")
  expect_identical(check_column_name("test", table_name = "local", exists = TRUE, conn), "test")
  expect_identical(check_column_name("e", table_name = "local", exists = FALSE, conn), "e")
})

test_that("check_column_blob", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  local <- data.frame(test = 1:2)
  expect_true(DBI::dbCreateTable(conn, "local", local))
  expect_true(add_blob_column("blob", table_name = "local", conn = conn))

  expect_error(check_column_blob("test", table_name = "local", conn),
               "column 'test' is not type BLOB")
  expect_error(check_column_blob("x", table_name = "local", conn),
               "column 'x' does not exist")
  expect_identical(check_column_blob("blob", table_name = "local", conn), "blob")
})

test_that("check_key", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  df <- data.frame(char = c("a", "b", "b"),
                   num = c(1.1, 2.2, 2.2),
                   key = c(1, 2, 3),
                   stringsAsFactors = FALSE)
  expect_true(DBI::dbWriteTable(conn, "df", df))
  key <- df[1,]
  key2 <- "a"
  key3 <- data.frame(num = 1.1, key = 2)

  expect_error(check_key(table_name = "df", key = key2, conn),
               "key must inherit from class data.frame")
  expect_error(check_key(table_name = "df", key = key3, conn),
               "filtering table by key must result in a single observation")
  expect_identical(check_key(table_name = "df", key = key, conn), key)
})

test_that("check_flob_query", {
 x <- list(NULL)
 expect_error(check_flob_query(x), "there is no flob to retrieve")
 expect_error(check_flob_query(x, "delete"), "there is no flob to delete")

 x <- flobr::flob_obj
 expect_identical(check_flob_query(x), x)
})
