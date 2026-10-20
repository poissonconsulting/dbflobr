# Save flobs.

Rename [`flobr::flob()`](https://rdrr.io/pkg/flobr/man/flob.html)s from
a SQLite database BLOB column and save to directory.

## Usage

``` r
save_flobs(
  column_name,
  table_name,
  conn,
  dir = ".",
  sep = "_-_",
  sub = FALSE,
  replace = FALSE,
  slob_ext = NULL
)
```

## Arguments

- column_name:

  A string of the name of the BLOB column.

- table_name:

  A string of the name of the existing table.

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

- slob_ext:

  A string of the file extension to use if slobs (serialized blobs) are
  encountered. If slob_ext = NULL slobs will be ignored.

## Value

An invisible named vector of the file names and new file names saved.

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
save_flobs("BlobColumn", "Table1", conn, dir)
#> Saving files to /tmp/RtmpJwYNuW
#> ✖ Row 1: no file found
#> ✔ Row 2: file flobr.pdf renamed to 2.pdf
DBI::dbDisconnect(conn)
```
