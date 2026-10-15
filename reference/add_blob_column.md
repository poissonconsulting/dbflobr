# Add blob column

Add named empty blob column to SQLite database

## Usage

``` r
add_blob_column(column_name, table_name, conn)
```

## Arguments

- column_name:

  A string of the name of the BLOB column.

- table_name:

  A string of the name of the existing table.

- conn:

  A SQLite connection object.

## Value

Modified SQLite database.

## Examples

``` r
conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)))
DBI::dbReadTable(conn, "Table1")
#>   IntColumn
#> 1         1
#> 2         2
add_blob_column("BlobColumn", "Table1", conn)
DBI::dbReadTable(conn, "Table1")
#>   IntColumn BlobColumn
#> 1         1       <NA>
#> 2         2       <NA>
DBI::dbDisconnect(conn)
```
