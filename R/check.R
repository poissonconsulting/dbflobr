# copied from readwritesqlite
check_sqlite_connection <- function(x, connected = NA, x_name = substitute(x), error = TRUE) {
  x_name <- chk_deparse(x_name)
  check_scalar(connected, values = c(TRUE, NA))
  check_flag(error)
  check_inherits(x, "SQLiteConnection", x_name = x_name)
  if(isTRUE(connected) && !dbIsValid(x)) {
    chk_fail(x_name, " must be connected", error = error)
  } else if(isFALSE(connected) && dbIsValid(x))
    chk_fail(x_name, " must be disconnected", error = error)
  invisible(x)
}

# modified from readwritesqlite
check_table_name <- function(table_name, conn) {
  check_string(table_name)

  table_exists <- table_exists(table_name, conn)
  if(!table_exists)
    err("table '", table_name, "' does not exist")

  table_name
}

check_column_name <- function(column_name, table_name, exists, conn){
  check_string(table_name)
  check_string(column_name)

  column_exists <- column_exists(column_name, table_name, conn)
  if(isTRUE(exists) && !column_exists)
    err("column '", column_name, "' does not exist")
  if(isFALSE(exists) && column_exists)
    err("column '", column_name, "' already exists")
  column_name
}

check_column_blob <- function(column_name, table_name, conn){
  check_column_name(column_name, table_name, exists = TRUE, conn)
  is_blob <- is_column_blob(column_name, table_name, conn)
  if(!is_blob)
    err("column '", column_name, "' is not type BLOB")
  column_name
}

check_key <- function(table_name, key, conn){
  check_data(key, nrow = 1L)
  x <- filter_key(table_name, key, conn)
  if(nrow(x) != 1L){
    err("filtering table by key must result in a single observation")
  }
  key
}

check_flob_query <- function(x, y = "retrieve"){
  if(is.null(unlist(x)))
    err("there is no flob to ", y)
  class(x) <- c("flob", "blob")
  flobr::check_flob(x)
}
