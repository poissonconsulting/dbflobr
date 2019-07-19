#' Write flob
#'
#' Write a flob to SQLite database table (column must be type BLOB).
#'
#'  A flob is a file that was read into binary in integer-mode as little endian,
#'  saved as the single element of a named list
#'  (where the name is the extension of the original file)
#'  and then serialized before being coerced into a blob.
#'
#'  Flobs are created with the `flobr` package.
#'
#' @param flob A flob.
#' @param column_name A string of the name of the BLOB column.
#' @param table_name A string of the name of the existing table.
#' @param key A data.frame whose columns and values are used to filter the
#' table to a single row (this in combination with the `column_name`
#' argument are used to target a single cell within the table to modify).
#' @param conn A SQLite connection object.
#' @param exists A flag specifying whether the column must already exist.
#' IF FALSE, a new BLOB column is created.
#'
#' @return Modified database.
#' @examples
#' library(flobr)
#' library(DBI)
#' flob <- flobr::flob(system.file("extdata", "flobr.pdf", package = "flobr"))
#' path <- "db.sqlite"
#' conn <- DBI::dbConnect(drv = RSQLite::SQLite(), path)
#' key <- data.frame(CharColumn = "a", IntColumn = 2L)
#' write_flob(flob, "BlobColumn", "Table1", key, conn)
#'
#' @export
write_flob <- function(flob, column_name, table_name, key, conn, exists = TRUE) {

  flobr::check_flob(flob)
  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_flag(exists)

  if(exists) {
    check_column_blob(column_name, table_name, conn)
  } else {
    add_blob_column(column_name, table_name, conn)
  }

  check_key(table_name, key, conn)

  sql <- glue_sql("UPDATE {`table_name`} SET {`column_name`}",
                  column_name = column_name,
                  table_name = table_name,
                  .con = conn)
  sql <- glue("{sql} = {collapse_flob(flob)} WHERE {safe_key(key, conn)}")

  execute(sql, conn)
  invisible(TRUE)
}

#' Read flob
#'
#' Read a blob from SQLite database into flob.
#'
#' @inheritParams write_flob
#'
#' @return A flob.
#' @examples
#' library(DBI)
#' path <- "db.sqlite"
#' conn <- DBI::dbConnect(drv = RSQLite::SQLite(), path)
#' key <- data.frame(CharColumn = "a", IntColumn = 2L)
#' read_flob("BlobColumn", "Table1", key, conn)
#' @export
read_flob <- function(column_name, table_name, key, conn) {

  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_column_blob(column_name, table_name, conn)
  check_key(table_name, key, conn)

  x <- query_flob(column_name, table_name, key, conn)
  x <- check_flob_query(x)
  x
}

#' Delete blob
#'
#' Delete a blob from SQLite database.
#'
#' @inheritParams write_flob
#'
#' @return A flob.
#' @examples
#' library(DBI)
#' path <- "db.sqlite"
#' conn <- DBI::dbConnect(drv = RSQLite::SQLite(), path)
#' key <- data.frame(CharColumn = "a", IntColumn = 2L)
#' delete_flob("BlobColumn", "Table1", key, conn)
#' @export
delete_flob <- function(column_name, table_name, key, conn) {

  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_column_blob(column_name, table_name, conn)
  check_key(table_name, key, conn)

  # first check that there is a flob there
  x <- query_flob(column_name, table_name, key, conn)
  x <- check_flob_query(x, "delete")

  sql <- glue_sql("UPDATE {`table_name`} SET {`column_name`}",
                  column_name = column_name,
                  table_name = table_name,
                  .con = conn)
  sql <- glue("{sql} = NULL WHERE {safe_key(key, conn)}")

  execute(sql, conn)
  invisible(TRUE)
}
