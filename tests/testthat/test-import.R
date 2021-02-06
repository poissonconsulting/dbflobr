test_that("import_flobs works", {
  path <- withr::local_tempdir()

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
  expect_length(list_files(path), 4L)

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
  expect_identical(names(x), basename(list_files(path, recursive = TRUE, pattern = ".*")))

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
  write_flob(flob, "New", "df", key = data.frame(char = "a", num = 1), conn)
  write_flob(flob, "New2", "df", key = data.frame(char = "a", num = 2.1), conn)
  write_flob(flob, "New", "df", key = data.frame(char = "b"), conn)
  write_flob(flob, "New", "df2", key = data.frame(char = "b"), conn)

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

  x <- import_all_flobs(conn, path, exists = TRUE, replace = TRUE, pattern = "b")

  expect_identical(sum(unlist(x)), 2L)
  expect_length(x, 3)
  expect_identical(names(unlist(x)), c("df/New.b_-_1.pdf",
                                       "df2/New.b.pdf"))
})

test_that("import_all_flobs requires unique", {
  path <- withr::local_tempdir()

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  # 2 column pk
  DBI::dbExecute(conn,
                 "CREATE TABLE df (
                char TEXT NOT NULL,
                num REAL NOT NULL,
                PRIMARY KEY (char, num))")

  # one column pk with two blob cols
  DBI::dbWriteTable(conn, "df",
                    data.frame(char = c("a", "a", "b"), num = c(1, 2.1, 1)),
                    append = TRUE)

  flob <- flobr::flob_obj
  write_flob(flob, "New", "df", key = data.frame(char = "a", num = 1), conn)

  expect_identical(save_all_flobs(conn = conn, dir = path), list(`df/New` = c(flobr.pdf = "a_-_1.pdf")))

  expect_error(import_flobs("New", "df", conn = conn, dir = path),
               "`New` must not already exist in table `df`.")

  list.files(file.path(path), recursive = TRUE)

  expect_identical(import_flobs("New", "df", conn = conn, dir = path, exists = TRUE),
                   structure(logical(0), .Names = character(0)))

  expect_identical(import_flobs("New", "df", conn = conn, dir = file.path(path, "df", "New"), exists = TRUE, replace = TRUE),
                   c("a_-_1.pdf" = TRUE))

  expect_identical(import_flobs("New", "df", conn = conn, dir =path, exists = TRUE, replace = TRUE, recursive = TRUE),
                   c("a_-_1.pdf" = TRUE))

  save_all_flobs(conn = conn, dir = file.path(path, "sub"))

  expect_identical(import_flobs("New", "df", conn = conn, dir = file.path(path, "df", "New"), exists = TRUE, replace = TRUE),
                   c("a_-_1.pdf" = TRUE))

  expect_identical(import_flobs("New", "df", conn = conn, dir = file.path(path, "sub", "df", "New"), exists = TRUE, replace = TRUE),
                   c("a_-_1.pdf" = TRUE))

  expect_error(import_flobs("New", "df", conn = conn, dir = path, exists = TRUE, recursive = TRUE),
               "File names must be unique.")
})

test_that("import_all_flobs is actually recursive", {
  path <- withr::local_tempdir()

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  # 2 column pk
  DBI::dbExecute(conn,
                 "CREATE TABLE df (
                char TEXT NOT NULL,
                num REAL NOT NULL,
                PRIMARY KEY (char, num))")

  # one column pk with two blob cols
  DBI::dbWriteTable(conn, "df",
                    data.frame(char = c("a", "a", "b"), num = c(1, 2.1, 1)),
                    append = TRUE)

  flob <- flobr::flob_obj
  write_flob(flob, "New", "df", key = data.frame(char = "a", num = 1), conn)

  expect_identical(save_all_flobs(conn = conn, dir = file.path(path, "sub")), list(`df/New` = c(flobr.pdf = "a_-_1.pdf")))

  expect_identical(import_flobs("New", "df", conn = conn, dir = path, exists = TRUE, replace = TRUE, recursive = TRUE),
                   c("a_-_1.pdf" = TRUE))
})

test_that("import_flobs works with subdirectory", {
  path <- withr::local_tempdir()

  dir.create(file.path(path, "a_-_1"))
  dir.create(file.path(path, "b_-_2"))
  dir.create(file.path(path, "b_-_3"))

  df <- data.frame(a = 1)

  write.csv(df, file.path(path, "a_-_1", "data.csv"))
  write.csv(df, file.path(path, "b_-_2", "data.csv"))
  write.csv(df, file.path(path, "b_-_3", "data.csv"))

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  # 2 column pk
  DBI::dbExecute(conn,
                 "CREATE TABLE df (
                char TEXT NOT NULL,
                int INTEGER NOT NULL,
                num REAL NOT NULL,
                PRIMARY KEY (char, int))")

  DBI::dbWriteTable(conn, "df",
                    data.frame(char = c("a", "b", "b"),
                               int = c(1, 2, 3),
                               num = c(1, 1, 1), stringsAsFactors = FALSE),
                    append = TRUE)

  expect_identical(list.files(path, recursive = TRUE),
                   c("a_-_1/data.csv", "b_-_2/data.csv", "b_-_3/data.csv"))

  expect_identical(import_flobs("New", "df", conn, path, sub = TRUE),
                   c(`a_-_1` = TRUE, `b_-_2` = TRUE, `b_-_3` = TRUE))

  expect_identical(import_flobs("New", "df", conn, path, sub = TRUE, exists = TRUE, replace = TRUE),
                   c(`a_-_1` = TRUE, `b_-_2` = TRUE, `b_-_3` = TRUE))

  unlink(file.path(path, "a_-_1", "data.csv"))

  expect_identical(import_flobs("New", "df", conn, path, sub = TRUE, exists = TRUE, replace = TRUE),
                   c(`b_-_2` = TRUE, `b_-_3` = TRUE))

  write.csv(df, file.path(path, "b_-_2", "data2.csv"))
  expect_error(import_flobs("New", "df", conn, path, sub = TRUE, exists = TRUE, replace = TRUE),
               "Directory names must be unique.")

  expect_identical(import_flobs("New", "df", conn, path, sub = TRUE, exists = TRUE, replace = TRUE, pattern = "data.csv"),
                   c(`b_-_2` = TRUE, `b_-_3` = TRUE))
})

test_that("import_flobs does not recurse beyond 1", {
  path <- withr::local_tempdir()

  dir.create(file.path(path, "extra", "a_-_1"), recursive = TRUE)
  dir.create(file.path(path, "extra", "b_-_2"), recursive = TRUE)
  dir.create(file.path(path, "extra", "b_-_3"), recursive = TRUE)

  df <- data.frame(a = 1)

  write.csv(df, file.path(path, "extra", "a_-_1", "data.csv"))
  write.csv(df, file.path(path, "extra", "b_-_2", "data.csv"))
  write.csv(df, file.path(path, "extra", "b_-_3", "data.csv"))

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  # 2 column pk
  DBI::dbExecute(conn,
                 "CREATE TABLE df (
                char TEXT NOT NULL,
                int INTEGER NOT NULL,
                num REAL NOT NULL,
                PRIMARY KEY (char, int))")

  DBI::dbWriteTable(conn, "df",
                    data.frame(char = c("a", "b", "b"),
                               int = c(1, 2, 3),
                               num = c(1, 1, 1), stringsAsFactors = FALSE),
                    append = TRUE)

  expect_identical(import_flobs("New", "df", conn, path, sub = TRUE),
                   structure(logical(0), .Names = character(0)))

  expect_identical(import_flobs("New2", "df", conn, file.path(path, "extra"), sub = TRUE),
                   c(`a_-_1` = TRUE, `b_-_2` = TRUE, `b_-_3` = TRUE))
})


test_that("import_flobs sub = TRUE", {
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

  x <- save_all_flobs(conn = conn, dir = file.path(path, "dump"), sub = TRUE, geometry = TRUE)
  expect_identical(x, list(`df/geometry` = c(flobr.pdf = "a_-_1.pdf", flobr.pdf = "b_-_1.pdf"
  ), `df2/geometry2` = structure(logical(0), .Names = character(0))))

  expect_identical(import_all_flobs(conn = conn, dir = file.path(path, "dump"), sub = TRUE,
                   exists = TRUE),
                   list(`df/geometry` = c(`a_-_1` = FALSE, `b_-_1` = FALSE), `df2/geometry2` = structure(logical(0), .Names = character(0))))

  expect_identical(import_all_flobs(conn = conn, dir = file.path(path, "dump"), sub = TRUE,
                                    exists = TRUE, replace = TRUE),
                   list(`df/geometry` = c(`a_-_1` = TRUE, `b_-_1` = TRUE), `df2/geometry2` = structure(logical(0), .Names = character(0))))
})

test_that("import_flobs sub = NA", {
  path <- withr::local_tempdir()

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  # 2 column pk
  DBI::dbExecute(conn,
                 "CREATE TABLE df (
                char TEXT NOT NULL,
                num REAL NOT NULL,
                PRIMARY KEY (char, num))")

  # one column pk with two blob cols
  DBI::dbWriteTable(conn, "df",
                    data.frame(char = c("a", "a", "b"), num = c(1, 2.1, 1)),
                    append = TRUE)

  flob <- flobr::flob_obj
  write_flob(flob, "geometry", "df", key = data.frame(char = "a", num = 1), conn)
  write_flob(flob, "geometry", "df", key = data.frame(char = "b"), conn)

  x <- save_all_flobs(conn = conn, dir = file.path(path, "dump"), sub = NA, geometry = TRUE)
  expect_identical(x, list(`df/geometry` = c(flobr.pdf = "a_-_1.pdf", flobr.pdf = "b_-_1.pdf"
  )))

  expect_identical(import_flobs("geometry", "df", conn = conn, dir = file.path(path, "dump", "df", "geometry"), sub = NA, exists = TRUE),
                   c(`a_-_1` = FALSE, `a_-_2.1` = TRUE, `b_-_1` = FALSE))

  expect_identical(import_flobs("geometry", "df", conn = conn, dir = file.path(path, "dump", "df", "geometry"), sub = NA, exists = TRUE, replace = TRUE),
                   c(`a_-_1` = TRUE, `a_-_2.1` = TRUE, `b_-_1` = TRUE))

  unlink(file.path(path, "dump", "df", "geometry", "b_-_1", "b_-_1.pdf"))

  expect_is(read_flob("geometry", "df", conn = conn, key = data.frame(char = "b", num = 1)), "flob")

  expect_identical(import_flobs("geometry", "df", conn = conn, dir = file.path(path, "dump", "df", "geometry"), sub = NA, exists = TRUE, replace = TRUE),
                   c(`a_-_1` = TRUE, `a_-_2.1` = TRUE, `b_-_1` = TRUE))

  expect_error(read_flob("geometry", "df", conn = conn, key = data.frame(char = "b", num = 1)))
})

test_that("import_all_flobs works", {
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
                    data.frame(char = c("a", "a", "b"), num = c(1, 21, 1)),
                    append = TRUE)
  DBI::dbWriteTable(conn, "df2", data.frame(char = c("a", "b")), append = TRUE)

  flob <- flobr::flob_obj
  write_flob(flob, "New", "df", key = data.frame(char = "a", num = 1), conn)
  write_flob(flob, "New2", "df", key = data.frame(char = "a", num = 21), conn)
  write_flob(flob, "New", "df", key = data.frame(char = "b"), conn)
  write_flob(flob, "New", "df2", key = data.frame(char = "b"), conn)

  save_all_flobs(conn = conn, dir = path, sub = NA)

  expect_identical(import_all_flobs(conn, path, exists = TRUE, sub = TRUE),
                   list(`df/New` = c(`a_-_1` = FALSE, `b_-_1` = FALSE), `df/New2` = c(`a_-_21` = FALSE),
                        `df2/New` = c(b = FALSE)))

  expect_identical(import_all_flobs(conn, path, exists = TRUE, replace = TRUE, sub = TRUE),
                   list(`df/New` = c(`a_-_1` = TRUE, `b_-_1` = TRUE), `df/New2` = c(`a_-_21` = TRUE),
                        `df2/New` = c(b = TRUE)))

  list.files(path, recursive = TRUE, include.dirs = TRUE)

  import_flobs("New2", "df", conn = conn, dir = file.path(path, "df", "New2"), sub = TRUE, exists = TRUE, replace = TRUE)

  expect_identical(import_all_flobs(conn, path, exists = TRUE, sub = NA),
                   list(`df/New` = c(`a_-_1` = FALSE, `a_-_21` = TRUE, `b_-_1` = FALSE
                   ), `df/New2` = c(`a_-_1` = TRUE, `a_-_21` = FALSE, `b_-_1` = TRUE
                   ), `df2/New` = c(a = TRUE, b = FALSE)))

  expect_identical(import_all_flobs(conn, path, exists = TRUE, replace = TRUE, sub = NA),
                   list(`df/New` = c(`a_-_1` = TRUE, `a_-_21` = TRUE, `b_-_1` = TRUE
                   ), `df/New2` = c(`a_-_1` = TRUE, `a_-_21` = TRUE, `b_-_1` = TRUE
                   ), `df2/New` = c(a = TRUE, b = TRUE)))
})

test_that("import_all_flobs works with .", {
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
  write_flob(flob, "New", "df", key = data.frame(char = "a", num = 1), conn)
  write_flob(flob, "New2", "df", key = data.frame(char = "a", num = 2.1), conn)
  write_flob(flob, "New", "df", key = data.frame(char = "b"), conn)
  write_flob(flob, "New", "df2", key = data.frame(char = "b"), conn)

  save_all_flobs(conn = conn, dir = path, sub = NA)

  expect_identical(import_all_flobs(conn, path, exists = TRUE, sub = TRUE),
                   list(`df/New` = c(`a_-_1` = FALSE, `b_-_1` = FALSE), `df/New2` = c(`a_-_2.1` = FALSE),
                        `df2/New` = c(b = FALSE)))

  # why is it false for a_-_2.1!! (should be true)
  expect_identical(import_all_flobs(conn, path, exists = TRUE, replace = TRUE, sub = TRUE),
                   list(`df/New` = c(`a_-_1` = TRUE, `b_-_1` = TRUE), `df/New2` = c(`a_-_2.1` = FALSE),
                        `df2/New` = c(b = TRUE)))

  list.files(path, recursive = TRUE, include.dirs = TRUE)

  import_flobs("New2", "df", conn = conn, dir = file.path(path, "df", "New2"), sub = TRUE, exists = TRUE, replace = TRUE)

  expect_identical(import_all_flobs(conn, path, exists = TRUE, sub = NA),
                   list(`df/New` = c(`a_-_1` = FALSE, `a_-_2.1` = TRUE, `b_-_1` = FALSE
                   ), `df/New2` = c(`a_-_1` = TRUE, `a_-_2.1` = FALSE, `b_-_1` = TRUE
                   ), `df2/New` = c(a = TRUE, b = FALSE)))

  # why is it false for second a_-_2.1!! (should be true)
  expect_identical(import_all_flobs(conn, path, exists = TRUE, replace = TRUE, sub = NA),
                   list(`df/New` = c(`a_-_1` = TRUE, `a_-_2.1` = TRUE, `b_-_1` = TRUE
                   ), `df/New2` = c(`a_-_1` = TRUE, `a_-_2.1` = FALSE, `b_-_1` = TRUE
                   ), `df2/New` = c(a = TRUE, b = TRUE)))
})
