check_sqlite_connection <- function(x, x_name = substitute(x)) {
  if (!is.character(x)) x_name <- deparse(x_name)
  if (!is_sqlite_connection(x))
    err(x_name, " must an SQLiteConnection object")
  invisible(x)
}

check_column_name <- function(conn, table_name, column_name){
  columns <- DBI::dbListFields(conn, table_name)
  if(!(column_name %in% columns))
    err("'", column_name, "' is not an existing column")
}

check_table_name <- function(conn, table_name){
  if (!DBI::dbExistsTable(conn, table_name))
    err("'", table_name, "' is not an existing table")
}

check_column_blob <- function(conn, table_name, column_name){
  sql <- "SELECT * FROM ?table_name LIMIT 1"
  query <- DBI::sqlInterpolate(conn, sql,
                               table_name = table_name)
  result <- DBI::dbSendQuery(conn = conn, query)
  types <- DBI::dbColumnInfo(result)
  type <- types$type[types$name == column_name]
  if(type != "list")
    err("'", column_name, "' must be type 'list'")
  DBI::dbClearResult(result)
}

check_rowid_column <- function(conn, table_name) {
  sql <- "SELECT rowid FROM ?table_name LIMIT 1"
  query <- DBI::sqlInterpolate(conn, sql,
                          table_name = table_name)
  x <- try(DBI::dbGetQuery(conn = conn, query), silent = TRUE)
  if(any(class(x) == "try-error"))
    err("rowid column does not exist")
}

check_rowid <- function(conn, table_name, rowid){
  check_rowid_column(conn, table_name)
  sql <- "SELECT rowid FROM ?table_name"
  query <- DBI::sqlInterpolate(conn, sql,
                          table_name = table_name)
  rowids <- DBI::dbGetQuery(conn, query)$rowid
  if(!(rowid %in% rowids))
    err("rowid does not exist")
}
