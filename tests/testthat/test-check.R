context("check")

test_that("check_sqlite_connection", {
  expect_error(
    check_sqlite_connection(1))
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

  expect_identical(check_sqlite_connection(conn), conn)
  expect_identical(check_sqlite_connection(conn, connected = TRUE), conn)
  expect_error(
    check_sqlite_connection(conn, connected = FALSE))
  DBI::dbDisconnect(conn)

  expect_identical(check_sqlite_connection(conn), conn)
  expect_error(
    check_sqlite_connection(conn, connected = TRUE))
  expect_identical(check_sqlite_connection(conn, connected = FALSE), conn)
})

test_that("check_table_name", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  local <- data.frame(x = 1:2)
  expect_true(DBI::dbCreateTable(conn, "local", local))

  expect_error(
    check_table_name(1, conn))
  expect_error(
    check_table_name("e", conn))
  expect_identical(check_table_name("local", conn), "local")
})

test_that("check_column_name", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  local <- data.frame(test = 1:2)
  expect_true(DBI::dbCreateTable(conn, "local", local))

  expect_error(
    check_column_name("test", table_name = 1, exists = TRUE, conn))
  expect_error(
    check_column_name("test", table_name = "e", exists = TRUE, conn))
  expect_error(
    check_column_name(1, table_name = "local", exists = TRUE, conn))
  expect_error(
    check_column_name("e", table_name = "local", exists = TRUE, conn))
  expect_identical(check_column_name("test", table_name = "local", exists = TRUE, conn), "test")
  expect_identical(check_column_name("e", table_name = "local", exists = FALSE, conn), "e")
})

test_that("check_column_blob", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  local <- data.frame(test = 1:2)
  expect_true(DBI::dbCreateTable(conn, "local", local))
  expect_true(add_blob_column("blob", table_name = "local", conn = conn))

  expect_error(
    check_column_blob("test", table_name = "local", conn))
  expect_error(
    check_column_blob("x", table_name = "local", conn))
  expect_identical(check_column_blob("blob", table_name = "local", conn), "blob")
})

test_that("check_key", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  df <- data.frame(
    char = c("a", "b", "b"),
    num = c(1.1, 2.2, 2.2),
    key = c(1, 2, 3),
    stringsAsFactors = FALSE
  )
  expect_true(DBI::dbWriteTable(conn, "df", df))
  key <- df[1, ]
  key2 <- "a"
  key3 <- data.frame(num = 1.1, key = 2)

  expect_error(
    check_key(table_name = "df", key = key2, conn))
  expect_error(
    check_key(table_name = "df", key = key3, conn))
  expect_identical(check_key(table_name = "df", key = key, conn), key)
})

test_that("check_flob_query", {
  x <- list(NULL)
  expect_error(check_flob_query(x))
  expect_error(check_flob_query(x))

  x <- flobr::flob_obj
  expect_identical(check_flob_query(x), x)
})
