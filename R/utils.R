#' Add blob column
#'
#' Add named empty blob column to SQLite database
#'
#' @inheritParams write_flob
#'
#' @return Modified SQLite database.
#' @export
#' @examples
#' conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)))
#' DBI::dbReadTable(conn, "Table1")
#' add_blob_column("BlobColumn", "Table1", conn)
#' DBI::dbReadTable(conn, "Table1")
#' DBI::dbDisconnect(conn)
add_blob_column <- function(column_name, table_name, conn) {

  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_column_name(column_name, table_name, exists = FALSE, conn)

  sql <- "ALTER TABLE ?table_name ADD ?column_name BLOB"
  sql <- sql_interpolate(sql, conn, table_name = table_name,
                         column_name = column_name)

  execute(sql, conn)
  invisible(TRUE)
}
