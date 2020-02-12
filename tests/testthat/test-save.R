test_that("save_flobs works", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  # 2 column pk
  DBI::dbGetQuery(conn,
                  "CREATE TABLE df (
                char TEXT NOT NULL,
                num REAL NOT NULL,
                PRIMARY KEY (char, num))")

  # one column pk with empty
  DBI::dbGetQuery(conn,
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

  expect_error(save_flobs("yup", "df", conn, path))
  expect_error(save_flobs("New", "df3", conn, path))
  expect_error(save_flobs("New", "df", "conn", path))
  expect_error(save_flobs("New", "df", conn, 2))

  save_flobs("New", "df", conn, path)
  expect_identical(list.files(path, pattern = "pdf"), c("a_-_1.pdf",
                                                        "a_-_2.1.pdf",
                                                        "b_-_1.pdf"))
  # works when pk length 1 and some empty flobs
  save_flobs("New", "df2", conn, path)
  # regardless of order
  expect_true(all(list.files(path, pattern = "pdf") %in% c("a_-_1.pdf",
                                                        "a_-_2.1.pdf",
                                                        "b.pdf",
                                                        "b_-_1.pdf")))

  write_flob(flob, "New2", "df2", key = data.frame(char = "a"), conn)

  save_all_flobs(conn = conn, dir = path)
  expect_error(save_all_flobs("df3", path, conn))
  expect_error(save_all_flobs("df", path, "conn"))
  expect_error(save_all_flobs("df", 2, conn))

  expect_true(all(list.files(path, pattern = "pdf", recursive = TRUE) %in%
                   c("a_-_1.pdf", "a_-_2.1.pdf",
                     "b_-_1.pdf", "b.pdf", "df/New/a_-_1.pdf",
                     "df/New/a_-_2.1.pdf", "df/New/b_-_1.pdf",
                     "df2/New/b.pdf", "df2/New2/a.pdf")))

  expect_identical(save_all_flobs("df2", conn, path), path)

})

