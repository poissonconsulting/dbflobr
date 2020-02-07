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

  save_flobs("New", "df", path, conn)
  expect_identical(list.files(path, pattern = "pdf"), c("char-num_a-1.pdf",
                                                        "char-num_a-2.1.pdf",
                                                        "char-num_b-1.pdf"))
  # works when pk length 1 and some empty flobs
  save_flobs("New", "df2", path, conn)
  expect_identical(list.files(path, pattern = "pdf"), c("char_b.pdf",
                                                        "char-num_a-1.pdf",
                                                        "char-num_a-2.1.pdf",
                                                        "char-num_b-1.pdf"))

})

