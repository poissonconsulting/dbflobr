#' Write flob
#'
#' Write a flob to a column (of type BLOB) in a database table.
#'
#' @param flob A flob.
#' @param column_name A string of the name of the BLOB column.
#' @param table_name A string of the name of the table.
#' @param key A data.frame with column names and values which filter table to a single row.
#' @param conn A connection object.
#' @param exists A flag specifying whether the column must already exist.
#' IF FALSE, a new BLOB column is created.
#'
#' @return Modified database.
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

  check_filter_key(table_name, key, conn)

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
#' @export
read_flob <- function(column_name, table_name, key, conn) {

  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_column_blob(column_name, table_name, conn)
  check_filter_key(table_name, key, conn)

  sql <- glue_sql("SELECT {`column_name`} FROM {`table_name`} WHERE",
                  column_name = column_name,
                  table_name = table_name,
                  .con = conn)
  sql <- glue("{sql} {safe_key(key, conn)}")

  x <- get_query(sql, conn)
  x <- unlist(x, recursive = FALSE)
  x <- check_flob_query(x)
  x
}
