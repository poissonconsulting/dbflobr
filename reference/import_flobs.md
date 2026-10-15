# Import flobs.

Import [`flobr::flob()`](https://rdrr.io/pkg/flobr/man/flob.html)s to
SQLite database column from directory. Values in file name are matched
to table primary key to determine where to write flob.

## Usage

``` r
import_flobs(
  column_name,
  table_name,
  conn,
  dir = ".",
  sep = "_-_",
  pattern = ".*",
  sub = FALSE,
  exists = FALSE,
  recursive = FALSE,
  replace = FALSE
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

  A string of the path to the directory to import files from.

- sep:

  A string of the separator between values in file names.

- pattern:

  A regular expression specifying the pattern file names must match.

- sub:

  A logical scalar specifying whether to import flobs based on their
  filename (sub = FALSE) or the name of their subdirectory (sub = TRUE)
  which must only contain 1 file. If sub = NA and replace = TRUE then
  the names of the subdirectories are used irrespective of whether they
  include files and existing flobs are deleted if the corresponding
  subdirectory is empty. If sub = TRUE or sub = NA then recursion is
  just one subfolder deep.

- exists:

  A logical scalar specifying whether the column must (TRUE) or mustn't
  (FALSE) already exist or whether it doesn't matter (NA). IF FALSE, a
  new BLOB column is created.

- recursive:

  A flag indicating whether to recurse into file directory (TRUE) or not
  (FALSE).

- replace:

  A flag indicating whether to replace existing flobs (TRUE) or not
  (FALSE).

## Value

An invisible named vector indicating file name and whether the file was
successfully written to database.

## Examples

``` r
conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
DBI::dbGetQuery(conn, "CREATE TABLE Table1 (CharColumn TEXT PRIMARY KEY NOT NULL)")
#> Warning: `dbGetQuery()`, `dbSendQuery()` and `dbFetch()` should only be used with `SELECT` queries. Did you mean `dbExecute()`, `dbSendStatement()` or `dbGetRowsAffected()`?
#> data frame with 0 columns and 0 rows
DBI::dbWriteTable(conn, "Table1", data.frame(CharColumn = c("a", "b")), append = TRUE)
key <- data.frame(CharColumn = "a", stringsAsFactors = FALSE)[0, , drop = FALSE]
dir <- tempdir()
write.csv(key, file.path(dir, "a.csv"))
import_flobs("BlobColumn", "Table1", conn, dir)
#> Writing files to database
#> ✔ File 1: a.csv written to database
#> ✖ File 2: can't write file19fe18f516e2 to database
#> ✖ File 3: can't write file19fe7e191217 to database
DBI::dbDisconnect(conn)
```
