vld_sqlite_conn <- function(x, connected = NA) {
  vld_s4_class(x, "SQLiteConnection") && (is.na(connected) || connected == dbIsValid(x))
}

# copied from readwritesqlite
check_sqlite_connection <- function(x, connected = NA, x_name = NULL) {
  if (vld_sqlite_conn(x, connected)) {
    return(invisible())
  }
  if (is.null(x_name)) x_name <- deparse_backtick_chk(substitute(x))
  chk_s4_class(x, "SQLiteConnection", x_name = x_name)
  if (vld_true(connected)) abort_chk(x_name, " must be connected.")
  abort_chk(x_name, " must be disconnected.")
}

# modified from readwritesqlite
check_table_name <- function(table_name, conn) {
  chk_string(table_name)

  table_exists <- table_exists(table_name, conn)
  if (!table_exists) {
    abort_chk("Can't find table `", table_name, "` in database.")
  }

  table_name
}

check_column_name <- function(column_name, table_name, exists, conn) {
  chk_string(table_name)
  chk_string(column_name)
  check_table_name(table_name, conn)

  column_exists <- column_exists(column_name, table_name, conn)
  if (isTRUE(exists) && !column_exists) {
    abort_chk("Can't find column `", column_name, "` in table `", table_name, "`.")
  }
  if (isFALSE(exists) && column_exists) {
    abort_chk("`", column_name, "` must not already exist in table `", table_name, "`.")
  }
  column_name
}

check_column_blob <- function(column_name, table_name, conn) {
  check_column_name(column_name, table_name, exists = TRUE, conn)
  is_blob <- is_column_blob(column_name, table_name, conn)
  if (!is_blob) {
    abort_chk("`", column_name, "` must be type BLOB.")
  }
  column_name
}

check_key <- function(table_name, key, conn) {
  check_data(key, nrow = 1L)
  x <- filter_key(table_name, key, conn)
  if (nrow(x) != 1L) {
    abort_chk("Filtering table by key must result in a single row.")
  }
  key
}

check_flob_query <- function(x) {
  if (is.null(unlist(x))) {
    abort_chk("Can't find flob in that location.")
  }
  class(x) <- c("flob", "blob")
  chk_flob(x)
  invisible(x)
}

check_pk <- function(table_name, conn){
  pk <- table_pk(table_name, conn)
  if(!length(pk)){
    abort_chk("Table `", table_name, "` must have a primary key.")
  }
  return(pk)
}
