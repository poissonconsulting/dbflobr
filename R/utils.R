#' Add blob column
#'
#' Add named empty blob column to SQLite database
#'
#' @param column_name A string of the name of the column.
#' @param table_name A string of the name of the table.
#' @param conn A connection object.
#'
#' @return Modified SQLite database.
#'
#' @export
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

.quotes <- "^(`|[[]|\")(.*)(`|[]]|\")$"

is_quoted <- function(x) grepl(.quotes, x)

to_upper <- function(x) {
  x <- as.character(x)
  is_quoted <- is_quoted(x)
  x[!is_quoted] <- toupper(x[!is_quoted])
  x
}

collapse_flob <- function(x) {
  flobr::check_flob(x)
  y <- glue_collapse(unlist(x), "")
  glue("x'{y}'")
}

err <- function (...) stop(..., call. = FALSE, domain = NA)

