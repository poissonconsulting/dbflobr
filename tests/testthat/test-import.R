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
                PRIMARY KEY (char, int))")

  DBI::dbWriteTable(conn, "df",
                    data.frame(char = c("a", "b", "b"),
                               int = c(1, 2, 3),
                               num = c(1, 1, 1), stringsAsFactors = FALSE),
                    append = TRUE)

  files <- list_files(path, recursive = FALSE)
  expect_length(list_files(path, recursive = FALSE), 3L)
  expect_length(list_files(path, recursive = TRUE), 4L)

  key2 <- data.frame(char = "a", int = 2, stringsAsFactors = FALSE)
  key <- key2[0,]
  key3 <- data.frame(char = "a", num = 2, stringsAsFactors = FALSE)[0,]

  column_name = "New"
  table_name = "df"
  dir = path
  exists = FALSE
  recursive = FALSE

  expect_error(import_flobs("New", "df2", key, conn, path),
               "Table `df2` must have a primary key.")

  # key must have 0 rows
  expect_error(import_flobs("New", "df", key2, conn, path))
  expect_error(import_flobs("New", "df", key3, conn, path),
               "key column names must include primary key column names.")

  x <- import_flobs("New2", "df", key, conn, path, recursive = FALSE)
  expect_true(all(x))
  expect_identical(names(x), basename(files))

  x <- import_flobs("New3", "df", key, conn, path, recursive = TRUE)
  expect_true(sum(x) == 3)
  expect_identical(names(x), basename(list_files(path, recursive = TRUE)))
})
