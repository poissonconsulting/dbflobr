# Delete flob

Delete a flob from a SQLite database.

## Usage

``` r
delete_flob(column_name, table_name, key, conn)
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

## Value

An invisible copy of the deleted flob.

## Examples

``` r
flob <- flobr::flob_obj
conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)))
key <- data.frame(IntColumn = 2L)
write_flob(flob, "BlobColumn", "Table1", key, conn, exists = FALSE)
DBI::dbReadTable(conn, "Table1")
#>   IntColumn      BlobColumn
#> 1         1            <NA>
#> 2         2 blob[133.85 kB]
delete_flob("BlobColumn", "Table1", key, conn)
DBI::dbReadTable(conn, "Table1")
#>   IntColumn BlobColumn
#> 1         1       <NA>
#> 2         2       <NA>
DBI::dbDisconnect(conn)
```
