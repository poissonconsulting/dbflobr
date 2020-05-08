test_that("write_flob works", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  df <- data.frame(
    char = c("a", "b", "b"),
    num = c(1.1, 2.2, 2.2),
    null = NA_character_,
    key = c(1, 2, 3),
    stringsAsFactors = FALSE
  )

  expect_true(DBI::dbWriteTable(conn, "df", df))

  key <- df[1, ]
  key2 <- data.frame(
    char = "a", num = 2.2,
    stringsAsFactors = FALSE
  )
  key3 <- data.frame(key = 3)

  flob <- flobr::flob_obj

  expect_error(write_flob(1, "flob",
                          table_name = "df",
                          exists = FALSE, key = key, conn = conn
  ), class = "chk_error")
  expect_error(
    write_flob(flob, "flob",
               table_name = "test",
               exists = FALSE, key = key, conn = conn
    ), class = "chk_error")
  expect_error(
    write_flob(flob, "flob",
               table_name = "df",
               exists = TRUE, key = key, conn = conn
    ), class = "chk_error")
  expect_error(
    write_flob(flob, "char",
               table_name = "df",
               exists = TRUE, key = key, conn = conn
    ), class = "chk_error")
  expect_error(
    write_flob(flob, "flob",
               table_name = "df",
               exists = FALSE, key = "a", conn = conn
    ), class = "chk_error")
  expect_error(
    write_flob(flob, "flob",
               table_name = "df",
               exists = FALSE, key = key2, conn = conn
    ), class = "chk_error")
  expect_error(
    write_flob(flob, "flob",
               table_name = "df",
               exists = TRUE, key = key2, conn = conn
    ), class = "chk_error")
  expect_is(write_flob(flob, "flob",
                       table_name = "df",
                       exists = TRUE, key = key, conn = conn
  ), "flob")

  df2 <- DBI::dbReadTable(conn, "df")
  expect_equal(df2$flob[1], flob, check.names = FALSE, check.attributes = FALSE)

  ### read flob
  expect_error(
    read_flob("flob", table_name = "test", key = key, conn = conn), class = "chk_error")
  expect_error(
    read_flob("blob", table_name = "df", key = key, conn = conn), class = "chk_error")
  expect_error(
    read_flob("flob", table_name = "df", key = key2, conn = conn), class = "chk_error")
  expect_error(
    read_flob("flob", table_name = "df", key = key3, conn = conn), class = "chk_error")

  flob2 <- read_flob("flob", table_name = "df", key = key, conn = conn)
  expect_identical(flobr::flob_ext(flob2), flobr::flob_ext(flob))
  expect_identical(flob2[[1]], flob[[1]])

  ### delete flobs
  expect_error(
    delete_flob("flob", table_name = "test", key = key, conn = conn), class = "chk_error")
  expect_error(
    delete_flob("blob", table_name = "df", key = key, conn = conn), class = "chk_error")
  expect_error(
    delete_flob("flob", table_name = "df", key = key2, conn = conn), class = "chk_error")

  expect_is(delete_flob("flob",
                        table_name = "df",
                        key = key, conn = conn
  ), "flob")
  expect_error(
    read_flob("flob", table_name = "df", key = key, conn = conn), class = "chk_error")
  expect_error(
    delete_flob("flob", table_name = "df", key = key, conn = conn), class = "chk_error")

})

test_that("write_flob column exists", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))
  expect_true(DBI::dbWriteTable(conn, "df", data.frame(Index = 1)))

  key <- data.frame(Index = 1)

  flob <- flobr::flob_obj
  expect_error(
    write_flob(flob, "New", "df", key, conn, exists = TRUE), class = "chk_error")
  expect_is(write_flob(flob, "New", "df", key, conn), "flob")
  expect_is(write_flob(flob, "New", "df", key, conn), "flob")
  expect_is(write_flob(flob, "New", "df", key, conn, exists = TRUE), "flob")
  expect_error(
    write_flob(flob, "New", "df", key, conn, exists = FALSE), class = "chk_error")
})

test_that("add_blob_column works", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  df <- data.frame(x = 1)

  expect_true(DBI::dbCreateTable(conn, "df", df))

  ## add blob_column
  expect_error(
    add_blob_column(table_name = "df", column_name = "x", conn = conn), class = "chk_error")
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

