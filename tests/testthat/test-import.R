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

  write.csv(mtcars, file.path(path, "a-1.csv"))
  write.csv(mtcars, file.path(path, "b-2.csv"))
  write.csv(mtcars, file.path(path, "b-3.csv"))
  write.csv(mtcars, file.path(inner_path, "b-3-2.csv"))

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  teardown(DBI::dbDisconnect(conn))

  # 2 column pk
  DBI::dbGetQuery(conn,
                  "CREATE TABLE df (
                char TEXT NOT NULL,
                int INTEGER NOT NULL,
                num REAL NOT NULL,
                PRIMARY KEY (char, int))")

  DBI::dbGetQuery(conn,
                  "CREATE TABLE df2 (
                char TEXT NOT NULL,
                num REAL NOT NULL)")

  # empty
  DBI::dbGetQuery(conn,
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

  key2 <- data.frame(char = "a", int = 2, stringsAsFactors = FALSE)
  key <- key2[0,]
  key3 <- data.frame(char = "a", num = 2, stringsAsFactors = FALSE)[0,]

  expect_error(import_flobs("New", "df2", key, conn, path),
               "Table `df2` must have a primary key.")

  # key must have 0 rows
  expect_error(import_flobs("New", "df", key2, conn, path))
  expect_error(import_flobs("New", "df", key3, conn, path),
               "key column names must include primary key column names.")

  x <- import_flobs("New2", "df", key, conn, path, recursive = FALSE)
  expect_true(all(x))
  expect_identical(names(x), basename(files))

  ### test replaces existing
  x <- import_flobs("New2", "df", key, conn, path, exists = TRUE, replace = TRUE, recursive = FALSE)
  expect_true(all(x))

  ### test wont replace existing
  x <- import_flobs("New2", "df", key, conn, path, exists = TRUE, replace = FALSE, recursive = FALSE)
  expect_true(!any(x))

  ### test nrows > length(files)
  expect_error(import_flobs("New3", "df", key, conn, path, recursive = TRUE),
               "Number of files must be less than or equal to number of rows in table.")

  ### test recursive
  unlink(file.path(path, "b-3.csv"))
  x <- import_flobs("New3", "df", key, conn, path, recursive = TRUE)
  expect_true(sum(x) == 2)
  expect_length(x, 3)
  expect_identical(names(x), basename(list_files(path, recursive = TRUE)))

  ### test pk of 1
  x <- import_flobs("New", "df3", key, conn, path, recursive = TRUE)
  expect_true(sum(x) == 2)
  expect_identical(names(x[x]), c("a-1.csv", "b-2.csv"))

})
