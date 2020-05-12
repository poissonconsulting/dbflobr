test_that("import_flobs works", {

  teardown(unlink(file.path(tempdir(), "dbflobr")))

  path <- file.path(tempdir(), "dbflobr")
  unlink(path, recursive = TRUE)
  dir.create(path)
  inner_path <- file.path(path, "dir1")
  inner_path2 <- file.path(inner_path, "dir2")
  inner_path3 <- file.path(path, "dir3")

  dir.create(inner_path)
  dir.create(inner_path2)
  dir.create(inner_path3)

  df <- data.frame(a = 1)

  write.csv(df, file.path(path, "a_-_1.csv"))
  write.csv(df, file.path(path, "b_-_2.csv"))
  write.csv(df, file.path(path, "b_-_3.csv"))
  write.csv(df, file.path(inner_path, "b_-_3_-_2.csv"))

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  # 2 column pk
  DBI::dbExecute(conn,
                  "CREATE TABLE df (
                char TEXT NOT NULL,
                int INTEGER NOT NULL,
                num REAL NOT NULL,
                PRIMARY KEY (char, int))")

  DBI::dbExecute(conn,
                  "CREATE TABLE df2 (
                char TEXT NOT NULL,
                num REAL NOT NULL)")

  # empty
  DBI::dbExecute(conn,
                  "CREATE TABLE df3 (
                char TEXT NOT NULL,
                int INTEGER NOT NULL,
                num REAL NOT NULL,
                PRIMARY KEY (char))")

  DBI::dbWriteTable(conn, "df",
                    data.frame(char = c("a", "b", "b"),
                               int = c(1, 2, 3),
                               num = c(1, 1, 1), stringsAsFactors = FALSE),
                    append = TRUE)

  DBI::dbWriteTable(conn, "df3",
                    data.frame(char = c("a", "b", "c"),
                               int = c(1, 2, 3),
                               num = c(1, 1, 1), stringsAsFactors = FALSE),
                    append = TRUE)

  files <- list_files(path, recursive = FALSE)
  expect_length(list_files(path, recursive = FALSE), 3L)
  expect_length(list_files(path, recursive = TRUE), 4L)

  expect_error(import_flobs("New", "df2", conn, path), class = "chk_error")

  x <- import_flobs("New2", "df", conn, path, recursive = FALSE)
  expect_true(all(x))
  expect_identical(names(x), basename(files))

  ### test replaces existing
  x <- import_flobs("New2", "df", conn, path, exists = TRUE, replace = TRUE, recursive = FALSE)
  expect_true(all(x))

  ### test wont replace existing
  x <- import_flobs("New2", "df", conn, path, exists = TRUE, recursive = FALSE)
  expect_true(!any(x))

  ### test recursive
  unlink(file.path(path, "b_-_3.csv"))
  x <- import_flobs("New3", "df", conn, path, recursive = TRUE)
  expect_true(sum(x) == 2)
  expect_length(x, 3)
  expect_identical(names(x), basename(list_files(path, recursive = TRUE)))

  write.csv(df, file.path(inner_path3, "a.csv"))
  write.csv(df, file.path(inner_path3, "b.csv"))
  write.csv(df, file.path(inner_path3, "b_-_1.csv"))

  ### test pk of 1
  x <- import_flobs("New", "df3", conn, inner_path3)
  expect_true(sum(x) == 2)
  expect_identical(names(x[x]), c("a.csv", "b.csv"))

  ### test sep arg
  x <- import_flobs("New", "df", conn, path, sep = "-")
  expect_true(sum(x) == 0)
  expect_identical(names(x), c("a_-_1.csv", "b_-_2.csv"))
  unlink(file.path(inner_path, "b_-_3_-_2.csv"))
  write.csv(df, file.path(inner_path, "b-3.csv"))

  x <- import_flobs("New4", "df", conn, inner_path, sep = "-")
  expect_true(sum(x) == 1)
  expect_identical(names(x), c("b-3.csv"))
})

test_that("import_all_flobs works", {

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
  write_flob(flob, "New2", "df", key = data.frame(char = "a", num = 2.1), conn)
  write_flob(flob, "New", "df", key = data.frame(char = "b"), conn)
  write_flob(flob, "New", "df2", key = data.frame(char = "b"), conn)

  ### works when pk length 2
  teardown(unlink(file.path(tempdir(), "dbflobr")))

  path <- file.path(tempdir(), "dbflobr")
  unlink(path, recursive = TRUE)
  dir.create(path)

  save_all_flobs(conn = conn, dir = path)

  expect_error(import_all_flobs(conn, path, exists = FALSE, replace = FALSE), class = "chk_error")

  x <- import_all_flobs(conn, path, exists = TRUE, replace = FALSE)
  expect_identical(sum(unlist(x)), 0L)
  expect_length(x, 3)
  expect_identical(names(unlist(x)), c("df/New.a_-_1.pdf",
                                       "df/New.b_-_1.pdf",
                                       "df/New2.a_-_2.1.pdf",
                                       "df2/New.b.pdf"))

  x <- import_all_flobs(conn, path, exists = TRUE, replace = TRUE)
  expect_identical(sum(unlist(x)), 4L)
  expect_length(x, 3)
  expect_identical(names(unlist(x)), c("df/New.a_-_1.pdf",
                                       "df/New.b_-_1.pdf",
                                       "df/New2.a_-_2.1.pdf",
                                       "df2/New.b.pdf"))

  x <- import_all_flobs(conn, path, sep = "-", exists = TRUE, replace = TRUE)
  expect_identical(sum(unlist(x)), 1L)

})
