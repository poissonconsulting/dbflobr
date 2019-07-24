context("package")

test_that("write flob works", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  df <- data.frame(char = c("a", "b", "b"),
                   num = c(1.1, 2.2, 2.2),
                   null = NA_character_,
                   key = c(1, 2, 3),
                   stringsAsFactors = FALSE)

  expect_true(DBI::dbWriteTable(conn, "df", df))

  key <- df[1,]
  key2 <- data.frame(char = "a", num = 2.2,
                     stringsAsFactors = FALSE)
  key3 <- data.frame(key = 3)

  flob <- flobr::flob_obj

  expect_error(write_flob(1, "flob", table_name =  "df",
                          exists = FALSE, key = key, conn = conn),
               "flob must inherit from class flob")
  expect_error(write_flob(flob, "flob", table_name =  "test",
                          exists = FALSE, key = key, conn = conn),
               "table 'test' does not exist")
  expect_error(write_flob(flob, "flob", table_name =  "df",
                          exists = TRUE, key = key, conn = conn),
               "column 'flob' does not exist")
  expect_error(write_flob(flob, "char", table_name =  "df",
                          exists = TRUE, key = key, conn = conn),
               "column 'char' is not type BLOB")
  expect_error(write_flob(flob, "flob", table_name =  "df",
                          exists = FALSE, key = "a", conn = conn),
               "key must inherit from class data.frame")
  expect_error(write_flob(flob, "flob", table_name =  "df",
                          exists = FALSE, key = key2, conn = conn),
               "column 'flob' already exists")
  expect_error(write_flob(flob, "flob", table_name =  "df",
                          exists = TRUE, key = key2, conn = conn),
               "filtering table by key must result in a single observation")
  expect_is(write_flob(flob, "flob", table_name =  "df",
                          exists = TRUE, key = key, conn = conn), "flob")

  df2 <- DBI::dbReadTable(conn, "df")
  expect_equal(df2$flob[1], flob, check.names = FALSE, check.attributes = FALSE)

  ### read flob
  expect_error(read_flob("flob", table_name = "test", key = key, conn = conn),
               "table 'test' does not exist")
  expect_error(read_flob("blob", table_name = "df", key = key, conn = conn),
               "column 'blob' does not exist")
  expect_error(read_flob("flob", table_name = "df", key = key2, conn = conn),
               "filtering table by key must result in a single observation")
  expect_error(read_flob("flob", table_name = "df", key = key3, conn = conn),
               "there is no flob to retrieve")

  flob2 <- read_flob("flob", table_name = "df", key = key, conn = conn)
  expect_identical(flobr::flob_ext(flob2), flobr::flob_ext(flob))
  expect_identical(flob2[[1]], flob[[1]])

  ### delete flob
  expect_error(delete_flob("flob", table_name = "test", key = key, conn = conn),
                "table 'test' does not exist")
  expect_error(delete_flob("blob", table_name = "df", key = key, conn = conn),
                "column 'blob' does not exist")
  expect_error(delete_flob("flob", table_name = "df", key = key2, conn = conn),
                "filtering table by key must result in a single observation")

  expect_is(delete_flob("flob", table_name =  "df",
                           key = key, conn = conn), "flob")
  expect_error(read_flob("flob", table_name = "df", key = key, conn = conn),
                "there is no flob to retrieve")
  expect_error(delete_flob("flob", table_name = "df", key = key, conn = conn),
                "there is no flob to delete")
})

test_that("write_flob column exists", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))
  expect_true(DBI::dbWriteTable(conn, "df", data.frame(Index = 1)))

  key <- data.frame(Index = 1)

  flob <- flobr::flob_obj
  expect_error(write_flob(flob, "New", "df", key, conn, exists = TRUE),
               "column 'New' does not exist")
  expect_is(write_flob(flob, "New", "df", key, conn), "flob")
  expect_is(write_flob(flob, "New", "df", key, conn), "flob")
  expect_is(write_flob(flob, "New", "df", key, conn, exists = TRUE), "flob")
  expect_error(write_flob(flob, "New", "df", key, conn, exists = FALSE),
               "column 'New' already exists")
})
