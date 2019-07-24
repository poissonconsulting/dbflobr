#' Write flob
#'
#' Write a \code{\link[flobr]{flob}} to a SQLite database.
#'
#' @param flob A flob.
#' @param column_name A string of the name of the BLOB column.
#' @param table_name A string of the name of the existing table.
#' @param key A data.frame whose columns and values are used to filter the
#' table to a single row (this in combination with the `column_name`
#' argument are used to target a single cell within the table to modify).
#' @param conn A SQLite connection object.
#' @param exists A logical scalar specifying whether the column must (TRUE) or
#' mustn't (FALSE) already exist or whether it doesn't matter (NA).
#' IF FALSE, a new BLOB column is created.
#' @return An invisible copy of flob.
#' @export
#' @examples
#' flob <- flobr::flob(system.file("extdata", "flobr.pdf", package = "flobr"))
#' conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)))
#' DBI::dbReadTable(conn, "Table1")
#' key <- data.frame(IntColumn = 2L)
#' write_flob(flob, "BlobColumn", "Table1", key, conn, exists = FALSE)
#' DBI::dbReadTable(conn, "Table1")
#' DBI::dbDisconnect(conn)
write_flob <- function(flob, column_name, table_name, key, conn, exists = NA) {
  flobr::check_flob(flob)
  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_scalar(exists, c(TRUE, NA))

  if(isTRUE(exists)) {
    check_column_blob(column_name, table_name, conn)
  } else if(isFALSE(exists) || !column_exists(column_name, table_name, conn))
    add_blob_column(column_name, table_name, conn)

  check_key(table_name, key, conn)

  sql <- glue_sql("UPDATE {`table_name`} SET {`column_name`}",
                  column_name = column_name,
                  table_name = table_name,
                  .con = conn)
  sql <- glue("{sql} = {collapse_flob(flob)} WHERE {safe_key(key, conn)}")

  execute(sql, conn)
  invisible(flob)
}

#' Read flob
#'
#' Read a \code{\link[flobr]{flob}} from a SQLite database.
#'
#' @inheritParams write_flob
#'
#' @return A flob.
#' @export
#' @examples
#' flob <- flobr::flob(system.file("extdata", "flobr.pdf", package = "flobr"))
#' conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)))
#' key <- data.frame(IntColumn = 2L)
#' write_flob(flob, "BlobColumn", "Table1", key, conn, exists = FALSE)
#' read_flob("BlobColumn", "Table1", key, conn)
#' DBI::dbDisconnect(conn)
read_flob <- function(column_name, table_name, key, conn) {
  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_column_blob(column_name, table_name, conn)
  check_key(table_name, key, conn)

  x <- query_flob(column_name, table_name, key, conn)
  check_flob_query(x)
}

#' Delete flob
#'
#' Delete a flob from a SQLite database.
#'
#' @inheritParams write_flob
#'
#' @return An invisible copy of the deleted flob.
#' @export
#' @examples
#' flob <- flobr::flob(system.file("extdata", "flobr.pdf", package = "flobr"))
#' conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)))
#' key <- data.frame(IntColumn = 2L)
#' write_flob(flob, "BlobColumn", "Table1", key, conn, exists = FALSE)
#' DBI::dbReadTable(conn, "Table1")
#' delete_flob("BlobColumn", "Table1", key, conn)
#' DBI::dbReadTable(conn, "Table1")
#' DBI::dbDisconnect(conn)
delete_flob <- function(column_name, table_name, key, conn) {
  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_column_blob(column_name, table_name, conn)
  check_key(table_name, key, conn)

  x <- query_flob(column_name, table_name, key, conn)
  x <- check_flob_query(x, "delete")

  sql <- glue_sql("UPDATE {`table_name`} SET {`column_name`}",
                  column_name = column_name,
                  table_name = table_name,
                  .con = conn)
  sql <- glue("{sql} = NULL WHERE {safe_key(key, conn)}")

  execute(sql, conn)
  invisible(x)
}
