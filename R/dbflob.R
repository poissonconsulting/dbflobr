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
#' flob <- flobr::flob_obj
#' conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)))
#' DBI::dbReadTable(conn, "Table1")
#' key <- data.frame(IntColumn = 2L)
#' write_flob(flob, "BlobColumn", "Table1", key, conn, exists = FALSE)
#' DBI::dbReadTable(conn, "Table1")
#' DBI::dbDisconnect(conn)
write_flob <- function(flob, column_name, table_name, key, conn, exists = NA) {
  chk_flob(flob)
  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  chk_lgl(exists)

  if (vld_true(exists)) {
    check_column_blob(column_name, table_name, conn)
  } else if (vld_false(exists) || !column_exists(column_name, table_name, conn)) {
    add_blob_column(column_name, table_name, conn)
  }

  check_key(table_name, key, conn)

  sql <- glue_sql("UPDATE {`table_name`} SET {`column_name`}",
    column_name = column_name,
    table_name = table_name,
    .con = conn
  )
  sql <- glue("{sql} = {collapse_flob(flob)} WHERE {safe_key(key, conn)}")

  execute(sql, conn)
  invisible(flob)
}

#' Read flob
#'
#' Read a \code{\link[flobr]{flob}} from a SQLite database.
#'
#' @inheritParams write_flob
#' @param slob A logical scalar specifying whether to process as slobs (serialized blobs) instead of flobs.
#' If NA, the function will adapt accordingly.
#'
#' @return A flob or blob.
#' @export
#' @examples
#' flob <- flobr::flob_obj
#' conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)))
#' key <- data.frame(IntColumn = 2L)
#' write_flob(flob, "BlobColumn", "Table1", key, conn, exists = FALSE)
#' read_flob("BlobColumn", "Table1", key, conn)
#' DBI::dbDisconnect(conn)
read_flob <- function(column_name, table_name, key, conn, slob = FALSE) {
  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_column_blob(column_name, table_name, conn)
  check_key(table_name, key, conn)
  chk_lgl(slob)

  x <- query_flob(column_name, table_name, key, conn)
  check_flob_query(x, slob = slob)
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
#' flob <- flobr::flob_obj
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
  x <- check_flob_query(x)

  sql <- glue_sql("UPDATE {`table_name`} SET {`column_name`}",
    column_name = column_name,
    table_name = table_name,
    .con = conn
  )
  sql <- glue("{sql} = NULL WHERE {safe_key(key, conn)}")

  execute(sql, conn)
  invisible(x)
}

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
  sql <- sql_interpolate(sql, conn,
                         table_name = table_name,
                         column_name = column_name
  )

  execute(sql, conn)
  invisible(TRUE)
}

