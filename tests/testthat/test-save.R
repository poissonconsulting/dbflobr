test_that("save_flobs works", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  # 2 column pk
  DBI::dbExecute(conn,
                  "CREATE TABLE df (
                char TEXT NOT NULL,
                num REAL NOT NULL,
                PRIMARY KEY (char, num))")

  # one column pk with empty
  DBI::dbExecute(conn,
                  "CREATE TABLE df2 (
                char TEXT PRIMARY KEY NOT NULL)")

  # one column pk with two blob cols
  DBI::dbWriteTable(conn, "df",
                    data.frame(char = c("a", "a", "b"), num = c(1, 2.1, 1)),
                    append = TRUE)
  DBI::dbWriteTable(conn, "df2", data.frame(char = c("a", "b")), append = TRUE)

  flob <- flobr::flob_obj
  write_flob(flob, "New", "df", key = data.frame(char = "a", num = 1), conn)
  write_flob(flob, "New", "df", key = data.frame(char = "a", num = 2.1), conn)
  write_flob(flob, "New", "df", key = data.frame(char = "b"), conn)
  write_flob(flob, "New", "df2", key = data.frame(char = "b"), conn)

  expect_identical(table_pk("df", conn), c("char", "num"))
  expect_identical(table_pk("df2", conn), c("char"))

  ### works when pk length 2
  teardown(unlink(file.path(tempdir(), "dbflobr")))

  path <- file.path(tempdir(), "dbflobr")
  unlink(path, recursive = TRUE)
  dir.create(path)

  # custom err messages
  expect_error(save_flobs("yup", "df", conn, path), class = "chk_error")
  expect_error(save_flobs("New", "df3", conn, path), class = "chk_error")

  # checkr err messages
  expect_error(save_flobs("New", "df", "conn", path), class = "chk_error")
  expect_error(save_flobs("New", "df", conn, 2), class = "chk_error")

  x <- save_flobs("New", "df", conn, path)
  expect_identical(names(x), c("flobr.pdf", "flobr.pdf", "flobr.pdf"))
  names(x) <- NULL
  expect_identical(list.files(path, pattern = "pdf"), x)

  # works when pk length 1 and some empty flobs
  y <- save_flobs("New", "df2", conn, path)
  names(y) <- NULL
  expect_identical(y, c(NA, "b.pdf"))
  # regardless of order
  expect_true(all(list.files(path, pattern = "pdf") %in% c("a_-_1.pdf",
                                                        "a_-_2.1.pdf",
                                                        "b.pdf",
                                                        "b_-_1.pdf")))

  write_flob(flob, "New2", "df2", key = data.frame(char = "a"), conn)

  y <- save_all_flobs(conn = conn, dir = path)
  expect_identical(names(y), c("df/New", "df2/New", "df2/New2"))
  z <- y$`df/New`
  names(z) <- NULL
  expect_identical(z, x)

  expect_error(save_all_flobs("df3", conn, path), class = "chk_error")
  expect_error(save_all_flobs("df", "conn", path), class = "chk_error")
  expect_error(save_all_flobs("df", conn, 2), class = "chk_error")

  expect_true(all(list.files(path, pattern = "pdf", recursive = TRUE) %in%
                   c("a_-_1.pdf", "a_-_2.1.pdf",
                     "b_-_1.pdf", "b.pdf", "df/New/a_-_1.pdf",
                     "df/New/a_-_2.1.pdf", "df/New/b_-_1.pdf",
                     "df2/New/b.pdf", "df2/New2/a.pdf")))

  x <- save_all_flobs("df2", conn, path)
  expect_identical(length(x), 2L)

})

