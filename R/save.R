#' Save flobs.
#'
#' Rename \code{\link[flobr]{flob}}s from a SQLite database BLOB column and save to directory.
#'
#' @inheritParams write_flob
#' @param dir A string of the path to the directory to save the files in.
#'
#' @return An invisible character string of the directory.
#' @export
#' @examples
#' flob <- flobr::flob_obj
#' conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' DBI::dbGetQuery(conn, "CREATE TABLE Table1 (IntColumn INTEGER PRIMARY KEY NOT NULL)")
#' DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)), append = TRUE)
#' key <- data.frame(IntColumn = 2L)
#' write_flob(flob, "BlobColumn", "Table1", key, conn, exists = FALSE)
#' dir <- tempdir()
#' save_flobs("BlobColumn", "Table1", conn, dir)
#' DBI::dbDisconnect(conn)
save_flobs <- function(column_name, table_name, conn, dir = "."){
  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_column_name(column_name, table_name, exists = TRUE, conn)
  check_string(dir)

  pk <- table_pk(table_name, conn)

  if(!length(pk))
    err("Table `", table_name, "` must have a PRIMARY KEY.")

  sql <- glue("SELECT {sql_pk(pk)} FROM ('{table_name}');")
  values <- get_query(sql, conn)

  ui_line(glue("Saving files to {ui_value(dir)}"))

  for(i in 1:nrow(values)){
    key <- values[i, , drop = FALSE]
    x <- try(read_flob(column_name, table_name, key, conn), silent = TRUE)

     if(!is_try_error(x)){
      filename <- flobr::flob_name(x)
      ext <- flobr::flob_ext(x)
      file <- glue("{filename}.{ext}")
      new_file <- filename_key(key)
      flobr::unflob(x, dir = dir, name = new_file)
      ui_done(glue("Row {i}: file {file} renamed to {new_file}.{ext}"))
    } else {
      ui_todo(glue("Row {i}: no file found"))
    }
  }
  return(invisible(dir))
}

#' Save all flobs.
#'
#' Rename \code{\link[flobr]{flob}}s from a SQLite database and save to directory.
#'
#' @inheritParams write_flob
#' @param dir A character string of the path to the directory to save files to.
#' @param table_name A vector of character strings indicating names of tables to save flobs from.
#' By default all tables are included.
#'
#' @return An invisible character string of the directory.
#' @export
#' @examples
#' flob <- flobr::flob_obj
#' conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' DBI::dbGetQuery(conn, "CREATE TABLE Table1 (IntColumn INTEGER PRIMARY KEY NOT NULL)")
#' DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)), append = TRUE)
#' key <- data.frame(IntColumn = 2L)
#' write_flob(flob, "BlobColumn", "Table1", key, conn, exists = FALSE)
#' dir <- tempdir()
#' save_all_flobs(conn = conn, dir = dir)
#' DBI::dbDisconnect(conn)
save_all_flobs <- function(table_name = NULL, conn, dir = "."){
  check_sqlite_connection(conn)
  checkor(check_table_name(table_name, conn), check_null(table_name))
  check_string(dir)

  if(is.null(table_name)){
    table_name <- table_names(conn)
  }

  for(i in table_name){
    cols <- blob_columns(i, conn)
    for(j in cols){
      path <- file.path(dir, i, j)
      if(!dir.exists(path))
        dir.create(path, recursive = TRUE)
      ui_line(glue("Table name: {ui_value(i)}"))
      ui_line(glue("Column name: {ui_value(j)}"))
      save_flobs(j, i, conn, path)
      ui_line("")
    }
  }
  invisible(dir)
}
