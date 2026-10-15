# Write flob

Write a [`flobr::flob()`](https://rdrr.io/pkg/flobr/man/flob.html) to a
SQLite database.

## Usage

``` r
write_flob(flob, column_name, table_name, key, conn, exists = NA)
```

## Arguments

- flob:

  A flob.

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

- exists:

  A logical scalar specifying whether the column must (TRUE) or mustn't
  (FALSE) already exist or whether it doesn't matter (NA). IF FALSE, a
  new BLOB column is created.

## Value

An invisible copy of flob.

## Examples

``` r
flob <- flobr::flob_obj
conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)))
DBI::dbReadTable(conn, "Table1")
#>   IntColumn
#> 1         1
#> 2         2
key <- data.frame(IntColumn = 2L)
write_flob(flob, "BlobColumn", "Table1", key, conn, exists = FALSE)
DBI::dbReadTable(conn, "Table1")
#>   IntColumn      BlobColumn
#> 1         1            <NA>
#> 2         2 blob[133.85 kB]
DBI::dbDisconnect(conn)
```
