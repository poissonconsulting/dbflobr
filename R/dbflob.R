#' Add blob column
#'
#' Add named empty blob column to SQLite database
#'
#' @param conn A connection object.
#' @param table_name A string of the name of the table.
#' @param column_name A string of the name of the column.
#'
#' @return Modified SQLite database.
#'
#' @export
blob_column <- function(table_name, column_name, conn) {
  check_string(table_name)
  check_string(column_name)
  check_sqlite_connection(conn)

  check_table_name(conn, table_name)
  columns <- DBI::dbListFields(conn, table_name)
  if(column_name %in% columns)
    err("'", column_name, "' already exists")

  sql <- paste("ALTER TABLE ?table_name ADD ?column_name BLOB")
  query <- DBI::sqlInterpolate(conn, sql, table_name = table_name,
                          column_name = column_name)

  result <- DBI::dbSendQuery(conn, query)
  DBI::dbClearResult(result)

  invisible(TRUE)
}

#' Collapse flob
#'
#' Collapse flob into format readable by SQLite
#'
#' @param x A flob.
#'
#' @return Modified SQLite database.
#' @export
flob_collapse <- function(x) {
  flobr::check_flob(x)
  paste0("x'", paste(unlist(x), collapse = ""), "'")
}

#' Write flob
#'
#' Write a flob to column of type BLOB in database.
#'
#' @param flob A flob.
#' @param conn A connection object.
#' @param table_name A string of the name of the table.
#' @param column_name A string of the name of the column.
#' @param rowid An integer of the table rowid.
#'
#' @return Modified database.
#' @export
write_flob <- function(flob, conn, table_name, column_name, rowid) {

  flobr::check_flob(flob)
  check_string(column_name)
  check_string(table_name)
  check_sqlite_connection(conn)
  check_int(rowid)

  check_table_name(conn, table_name)
  check_column_name(conn, table_name, column_name)
  check_column_blob(conn, table_name, column_name)
  check_rowid_column(conn, table_name)
  check_rowid(conn, table_name, rowid)

  sql <- paste("UPDATE ?table_name SET ?column_name = ", flob_collapse(flob), "WHERE rowid = ?rowid")
  query <- DBI::sqlInterpolate(conn, sql,
                               table_name = table_name,
                               column_name = column_name,
                               rowid = as.integer(rowid)[1])
  result <- DBI::dbSendQuery(conn, query)
  DBI::dbClearResult(result)

  invisible(TRUE)
}

#' Read flob
#'
#' Read a blob from SQLite database into flob.
#'
#' @param conn An connection object.
#' @param table_name A string of the name of the table.
#' @param column_name A string of the name of the column.
#' @param rowid An integer of the table rowid.
#'
#' @return A flob.
#' @export
read_flob <- function(conn, table_name, column_name, rowid) {

  check_string(column_name)
  check_string(table_name)
  check_sqlite_connection(conn)
  check_int(rowid)

  check_table_name(conn, table_name)
  check_column_name(conn, table_name, column_name)
  check_column_blob(conn, table_name, column_name)
  check_rowid_column(conn, table_name)
  check_rowid(conn, table_name, rowid)

  sql <- "SELECT ?column_name FROM ?table_name WHERE rowid = ?rowid"
  query <- DBI::sqlInterpolate(conn, sql,
                               column_name = column_name,
                               table_name = table_name,
                               rowid = as.integer(rowid)[1])
  query <- gsub("'", "", query)

  x <- DBI::dbGetQuery(conn, query)[[1]]
  if(is.na(x))
    err("There is no flob to retrieve")
  class(x) <- c("flob", "blob")
  flobr::check_flob(x)
  x
}
