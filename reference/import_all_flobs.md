# Import all flobs.

Import [`flobr::flob()`](https://rdrr.io/pkg/flobr/man/flob.html)s to
SQLite database from directory. Table and column names are matched to
directory names within main directory. Values in file names are matched
to table primary key to determine where to write flob.

## Usage

``` r
import_all_flobs(
  conn,
  dir = ".",
  sep = "_-_",
  pattern = ".*",
  sub = FALSE,
  exists = FALSE,
  replace = FALSE
)
```

## Arguments

- conn:

  A SQLite connection object.

- dir:

  A string of the path to the directory to import the files from. Files
  need to be within nested folders like 'table1/column1/a.csv'. This
  structure is created automatically if save_all_flobs() function is
  used.

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

- replace:

  A flag indicating whether to replace existing flobs (TRUE) or not
  (FALSE).

## Value

An invisible named list indicating directory path, file names and
whether files were successfully written to database.

## Examples

``` r
conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
DBI::dbGetQuery(conn, "CREATE TABLE Table1 (CharColumn TEXT PRIMARY KEY NOT NULL)")
#> Warning: `dbGetQuery()`, `dbSendQuery()` and `dbFetch()` should only be used with `SELECT` queries. Did you mean `dbExecute()`, `dbSendStatement()` or `dbGetRowsAffected()`?
#> data frame with 0 columns and 0 rows
DBI::dbWriteTable(conn, "Table1", data.frame(CharColumn = c("a", "b")), append = TRUE)
flob <- flobr::flob_obj
write_flob(flob, "BlobColumn", "Table1", data.frame(CharColumn = "a"), conn)
dir <- file.path(tempdir(), "import_all")
save_all_flobs(conn = conn, dir = dir)
#> Table name: "Table1"
#> Column name: "BlobColumn"
#> Saving files to /tmp/RtmpGq3pTv/import_all/Table1/BlobColumn
#> ✔ Row 1: file flobr.pdf renamed to a.pdf
#> ✖ Row 2: no file found
#> 
import_all_flobs(conn, dir, exists = TRUE, replace = TRUE)
#> Table name: "Table1"
#> Column name: "BlobColumn"
#> Writing files to database
#> ✔ File 1: a.pdf written to database
#> 
DBI::dbDisconnect(conn)
```
