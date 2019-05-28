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
