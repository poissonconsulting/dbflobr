test_that("save_flobs works", {
  path <- withr::local_tempdir()

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
  write_flob(flob, "geometry", "df", key = data.frame(char = "a", num = 1), conn)
  write_flob(flob, "geometry", "df", key = data.frame(char = "a", num = 2.1), conn)
  write_flob(flob, "geometry", "df", key = data.frame(char = "b"), conn)
  write_flob(flob, "geometry", "df2", key = data.frame(char = "b"), conn)

  expect_identical(table_pk("df", conn), c("char", "num"))
  expect_identical(table_pk("df2", conn), c("char"))

  # custom err messages
  expect_error(save_flobs("yup", "df", conn, path), class = "chk_error")
  expect_error(save_flobs("geometry", "df3", conn, path), class = "chk_error")

  # checkr err messages
  expect_error(save_flobs("geometry", "df", "conn", path), class = "chk_error")
  expect_error(save_flobs("geometry", "df", conn, 2), class = "chk_error")

  x <- save_flobs("geometry", "df", conn, path)
  expect_identical(names(x), c("flobr.pdf", "flobr.pdf", "flobr.pdf"))
  names(x) <- NULL
  expect_identical(list.files(path, pattern = "pdf"), x)

  # works when pk length 1 and some empty flobs
  y <- save_flobs("geometry", "df2", conn, path)
  names(y) <- NULL
  expect_identical(y, "b.pdf")
  # regardless of order
  expect_true(all(list.files(path, pattern = "pdf") %in% c("a_-_1.pdf",
                                                           "a_-_2.1.pdf",
                                                           "b.pdf",
                                                           "b_-_1.pdf")))

  write_flob(flob, "geometry2", "df2", key = data.frame(char = "a"), conn)

  y <- save_all_flobs(conn = conn, dir = path)
  expect_identical(names(y), "df2/geometry2")
  expect_error(save_all_flobs(conn = conn, dir = path, geometry = TRUE),
               "already exists")

  y <- save_all_flobs(conn = conn, dir = path, geometry = TRUE, replace = TRUE)

  expect_identical(names(y), c("df/geometry", "df2/geometry", "df2/geometry2"))
  z <- y$`df/geometry`
  names(z) <- NULL
  expect_identical(z, x)

  expect_error(save_all_flobs("df3", conn, path), class = "chk_error")
  expect_error(save_all_flobs("df", "conn", path), class = "chk_error")
  expect_error(save_all_flobs("df", conn, 2), class = "chk_error")

  expect_true(all(list.files(path, pattern = "pdf", recursive = TRUE) %in%
                    c("a_-_1.pdf", "a_-_2.1.pdf",
                      "b_-_1.pdf", "b.pdf", "df/geometry/a_-_1.pdf",
                      "df/geometry/a_-_2.1.pdf", "df/geometry/b_-_1.pdf",
                      "df2/geometry/b.pdf", "df2/geometry2/a.pdf")))

  expect_error(save_all_flobs("df2", conn, path, geometry = TRUE),
               "already exists")

  expect_identical(
    save_all_flobs("df2", conn, path, geometry = TRUE, replace = TRUE),
    list(`df2/geometry` = c(flobr.pdf = "b.pdf"), `df2/geometry2` = c(flobr.pdf = "a.pdf")))
})

test_that("save_flobs works with sub", {
  path <- withr::local_tempdir()

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
                char TEXT PRIMARY KEY NOT NULL,
                 geometry2 BLOB)")

  # one column pk with two blob cols
  DBI::dbWriteTable(conn, "df",
                    data.frame(char = c("a", "a", "b"), num = c(1, 2.1, 1)),
                    append = TRUE)
  DBI::dbWriteTable(conn, "df2", data.frame(char = c("a", "b")), append = TRUE)

  flob <- flobr::flob_obj
  write_flob(flob, "geometry", "df", key = data.frame(char = "a", num = 1), conn)
  write_flob(flob, "geometry", "df", key = data.frame(char = "b"), conn)

  x <- save_flobs("geometry", "df", conn, path)
  expect_identical(names(x), c("flobr.pdf", "flobr.pdf"))
  names(x) <- NULL
  expect_identical(list.files(path, pattern = "pdf"), x)

  expect_identical(list.files(path, recursive = TRUE),
                   c("a_-_1.pdf", "b_-_1.pdf"))

  unlink(path, recursive = TRUE)
  dir.create(path)
  x <- save_flobs("geometry", "df", conn, path, sub = TRUE)
  expect_identical(names(x), c("flobr.pdf", "flobr.pdf"))
  names(x) <- NULL
  expect_identical(list.files(path, recursive = TRUE, include.dirs = TRUE),
                   sort(c("a_-_1", "a_-_1/a_-_1.pdf", "b_-_1", "b_-_1/b_-_1.pdf")))

  unlink(path, recursive = TRUE)
  dir.create(path)
  x <- save_flobs("geometry", "df", conn, path, sub = NA)
  expect_identical(names(x), c("flobr.pdf", "flobr.pdf"))
  names(x) <- NULL
  expect_identical(list.files(path, recursive = TRUE, include.dirs = TRUE),
                   sort(c("a_-_1", "a_-_1/a_-_1.pdf", "a_-_2.1", "b_-_1", "b_-_1/b_-_1.pdf")))


  unlink(path, recursive = TRUE)
  dir.create(path)
  y <- save_all_flobs(conn = conn, dir = path, geometry = TRUE)
  expect_identical(names(y), sort(c("df/geometry", "df2/geometry2")))
  expect_identical(list.files(path, recursive = TRUE, include.dirs = TRUE),
                   sort(c("df", "df/geometry", "df/geometry/a_-_1.pdf", "df/geometry/b_-_1.pdf",
                          "df2", "df2/geometry2")))

  unlink(path, recursive = TRUE)
  dir.create(path)
  y <- save_all_flobs(conn = conn, dir = path, geometry = TRUE, sub = TRUE)
  expect_identical(names(y), sort(c("df/geometry", "df2/geometry2")))
  expect_identical(list.files(path, recursive = TRUE, include.dirs = TRUE),
                   sort(c("df", "df/geometry", "df/geometry/a_-_1", "df/geometry/a_-_1/a_-_1.pdf", "df/geometry/b_-_1", "df/geometry/b_-_1/b_-_1.pdf", "df2", "df2/geometry2")))

  unlink(path, recursive = TRUE)
  dir.create(path)
  y <- save_all_flobs(conn = conn, dir = path, geometry = TRUE, sub = NA)
  expect_identical(names(y), c("df/geometry", "df2/geometry2"))
  expect_identical(list.files(path, recursive = TRUE, include.dirs = TRUE),
                   sort(c(
                     "df", "df/geometry", "df/geometry/a_-_1",
                     "df/geometry/a_-_1/a_-_1.pdf",
                     "df/geometry/a_-_2.1", "df/geometry/b_-_1",
                     "df/geometry/b_-_1/b_-_1.pdf",
                     "df2", "df2/geometry2", "df2/geometry2/a", "df2/geometry2/b")))

  unlink(path, recursive = TRUE)
  dir.create(path)
  y <- save_all_flobs(conn = conn, dir = path, geometry = FALSE, sub = NA)
  expect_identical(names(y), c("df2/geometry2"))
  expect_identical(list.files(path, recursive = TRUE, include.dirs = TRUE),
                   sort(c("df2", "df2/geometry2", "df2/geometry2/a", "df2/geometry2/b")))
})


test_that("save_flob's slob compatibility", {
  path <- withr::local_tempdir()

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  DBI::dbExecute(conn,
                "CREATE TABLE df (
                PK TEXT NOT NULL,
                PRIMARY KEY (PK))")

  flob_df <- data.frame(PK = "flob")
  DBI::dbWriteTable(conn, "df", flob_df, append = TRUE)

  add_blob_column("FlobBlob", "df", conn)
  write_flob(flobr::flob_obj, "FlobBlob", "df", flob_df, conn)

  blob_df <- data.frame(PK = "blob", FlobBlob = flobr:::slob_obj)
  DBI::dbWriteTable(conn, "df", blob_df, append = TRUE)

  save_flobs("FlobBlob", "df", conn, dir = path, slob_ext = "pdf")

  expect_identical(list.files(path, recursive = TRUE, include.dirs = TRUE),
                   c("blob.pdf", "flob.pdf"))

  file.remove(file.path(path, "blob.pdf"))
  file.remove(file.path(path, "flob.pdf"))

  save_flobs("FlobBlob", "df", conn, dir = path, slob_ext = NULL)

  expect_identical(list.files(path, recursive = TRUE, include.dirs = TRUE),
                   c("flob.pdf"))

  file.remove(file.path(path, "flob.pdf"))

  save_flobs("FlobBlob", "df", conn, dir = path, slob_ext = "pdf", sub = TRUE)

  expect_identical(list.files(path, recursive = TRUE, include.dirs = TRUE),
                   c("blob", "blob/blob.pdf", "flob", "flob/flob.pdf"))

  unlink(file.path(path, "flob"), recursive = TRUE)
  unlink(file.path(path, "blob"), recursive = TRUE)

  save_flobs("FlobBlob", "df", conn, dir = path, slob_ext = NULL, sub = TRUE)

  expect_identical(list.files(path, recursive = TRUE, include.dirs = TRUE),
                   c("flob", "flob/flob.pdf"))
})
