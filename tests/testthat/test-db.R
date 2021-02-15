# borrowed from readwritesqlite
test_that("unquoted table names case insensitive in RSQLite", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  local <- data.frame(x = as.character(1:3))

  expect_false(table_exists("loCal", conn))
  expect_true(DBI::dbCreateTable(conn, "loCal", local))
  expect_true(table_exists("loCal", conn))
  expect_true(table_exists("LOCAL", conn))
  expect_identical(table_exists(c("loCal", "LOCAL"), conn), c(TRUE, TRUE))
  expect_true(DBI::dbCreateTable(conn, "`loCal`", local))
  expect_true(table_exists("`loCal`", conn))
  expect_false(table_exists("`LOCAL`", conn))

  skip_if_not_installed("RSQLite", "2.1.1.9003")
  expect_true(DBI::dbExistsTable(conn, "`loCal`"))
})

test_that("columns exist", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  local <- data.frame(x = as.character(1:3))

  expect_true(DBI::dbCreateTable(conn, "local", local))
  expect_true(column_exists("x", "local", conn))
  expect_true(column_exists("X", "local", conn))
  expect_false(column_exists("local", "local", conn))
})

test_that("columns type blob", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  local <- data.frame(x = as.character(1:3))
  expect_true(DBI::dbCreateTable(conn, "local", local))
  add_blob_column("flob", "local", conn)

  expect_identical(
    table_column_type(c("x", "flob"), "local", conn),
    c("TEXT", "BLOB")
  )

  expect_true(is_column_blob("flob", "local", conn))
  expect_false(is_column_blob("x", "local", conn))
})

test_that("filter key", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  df <- data.frame(
    "char" = c("a", "b", "b"),
    num = c(1.1, 2.2, 2.2),
    key = c(1, 2, 3),
    null = NA_character_,
    stringsAsFactors = FALSE
  )

  expect_true(DBI::dbWriteTable(conn, "df", df))

  ## safe_key
  key <- df[1, ]
  x <- safe_key(key, conn)
  expect_is(x, "glue")
  expect_length(x, 1L)

  x <- filter_key("df", key, conn)
  expect_identical(nrow(x), 1L)
  x <- filter_key("df", key = data.frame(char = "a", num = 2.2, stringsAsFactors = FALSE), conn)
  expect_identical(nrow(x), 0L)
})

# borrowed from readwritesqlite
test_that("table_info", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  local <- data.frame(
    logical = TRUE,
    date = as.Date("2000-01-01"),
    posixct = as.POSIXct("2001-01-02 03:04:05", tz = "Etc/GMT+8")
  )

  expect_true(DBI::dbCreateTable(conn, "local", local))

  table_info <- table_info("local", conn)
  expect_is(table_info, "data.frame")
  expect_identical(
    colnames(table_info),
    c("cid", "name", "type", "notnull", "dflt_value", "pk")
  )
  expect_identical(table_info$cid, 0:2)
  expect_identical(table_info$name, c("logical", "date", "posixct"))
  expect_identical(table_info$type, c("INTEGER", "REAL", "REAL"))
  expect_identical(table_info$notnull, rep(0L, 3))
  expect_identical(table_info$pk, rep(0L, 3))
})
