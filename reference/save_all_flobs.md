# Save all flobs.

Rename [`flobr::flob()`](https://rdrr.io/pkg/flobr/man/flob.html)s from
a SQLite database and save to directory.

## Usage

``` r
save_all_flobs(
  table_name = NULL,
  conn,
  dir = ".",
  sep = "_-_",
  sub = FALSE,
  replace = FALSE,
  geometry = FALSE
)
```

## Arguments

- table_name:

  A vector of character strings indicating names of tables to save flobs
  from. By default all tables are included.

- conn:

  A SQLite connection object.

- dir:

  A string of the path to the directory to save the files in.

- sep:

  A string of the separator used to construct file names from values.

- sub:

  A logical scalar specifying whether to save all existing files in a
  subdirectory of the same name (sub = TRUE) or all possible files in a
  subdirectory of the same name (sub = NA) or not nest files within a
  subdirectory (sub = FALSE).

- replace:

  A flag specifying whether to replace existing files. If sub = TRUE (or
  sub = NA) and replace = TRUE then all existing files within a
  subdirectory are deleted.

- geometry:

  A flag specifying whether to search columns named geometry for flobs.

## Value

An invisible named list of named vectors of the file names and new file
names saved.

## Examples

``` r
flob <- flobr::flob_obj
conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
DBI::dbGetQuery(conn, "CREATE TABLE Table1 (IntColumn INTEGER PRIMARY KEY NOT NULL)")
#> Warning: `dbGetQuery()`, `dbSendQuery()` and `dbFetch()` should only be used with `SELECT` queries. Did you mean `dbExecute()`, `dbSendStatement()` or `dbGetRowsAffected()`?
#> data frame with 0 columns and 0 rows
DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)), append = TRUE)
key <- data.frame(IntColumn = 2L)
write_flob(flob, "BlobColumn", "Table1", key, conn, exists = FALSE)
dir <- tempdir()
save_all_flobs(conn = conn, dir = dir)
#> Table name: "Table1"
#> Column name: "BlobColumn"
#> Saving files to /tmp/Rtmp9Xju1L/Table1/BlobColumn
#> ✖ Row 1: no file found
#> ✔ Row 2: file flobr.pdf renamed to 2.pdf
#> 
DBI::dbDisconnect(conn)
```
