# Read flob

Read a [`flobr::flob()`](https://rdrr.io/pkg/flobr/man/flob.html) from a
SQLite database.

## Usage

``` r
read_flob(column_name, table_name, key, conn, slob = FALSE)
```

## Arguments

- column_name:

  A string of the name of the BLOB column.

- table_name:

  A string of the name of the existing table.

- key:

  A data.frame whose columns and values are used to filter the table to
  a single row (this in combination with the `column_name` argument are
  used to target a single cell within the table to modify).

- conn:

  A SQLite connection object.

- slob:

  A logical scalar specifying whether to process as slobs (serialized
  blobs) instead of flobs. If NA, the function will adapt accordingly.

## Value

A flob or blob.

## Examples

``` r
flob <- flobr::flob_obj
conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)))
key <- data.frame(IntColumn = 2L)
write_flob(flob, "BlobColumn", "Table1", key, conn, exists = FALSE)
read_flob("BlobColumn", "Table1", key, conn)
DBI::dbDisconnect(conn)
```
