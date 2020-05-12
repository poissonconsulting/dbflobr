#' Save flobs.
#'
#' Rename \code{\link[flobr]{flob}}s from a SQLite database BLOB column and save to directory.
#'
#' @inheritParams write_flob
#' @param dir A string of the path to the directory to save the files in.
#' @param sep A string of the separator used to construct file names from values.
#'
#' @return An invisible named vector of the file names and new file names saved.
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
save_flobs <- function(column_name, table_name, conn, dir = ".", sep = "_-_"){
  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_column_name(column_name, table_name, exists = TRUE, conn)
  chk_string(dir)
  chk_string(sep)

  pk <- check_pk(table_name, conn)

  sql <- glue("SELECT {sql_pk(pk)} FROM ('{table_name}');")
  values <- get_query(sql, conn)

  ui_line(glue("Saving files to {ui_value(dir)}"))

  success <- vector()
  success_names <- vector()

  for(i in 1:nrow(values)){
    key <- values[i, , drop = FALSE]
    x <- try(read_flob(column_name, table_name, key, conn), silent = TRUE)

     if(!is_try_error(x)){
      filename <- flobr::flob_name(x)
      ext <- flobr::flob_ext(x)
      file <- glue("{filename}.{ext}")
      new_file <- create_filename(key, sep = sep)
      new_file_ext <- glue("{new_file}.{ext}")
      success[i] <- new_file_ext
      success_names[i] <- file
      new_file <- as.character(new_file)
      flobr::unflob(x, dir = dir, name = new_file)
      ui_done(glue("Row {i}: file {file} renamed to {new_file_ext}"))
    } else {
      ui_oops(glue("Row {i}: no file found"))
    }
  }
  names(success) <- success_names
  return(invisible(success))
}

#' Save all flobs.
#'
#' Rename \code{\link[flobr]{flob}}s from a SQLite database and save to directory.
#'
#' @inheritParams write_flob
#' @param table_name A vector of character strings indicating names of tables to save flobs from.
#' By default all tables are included.
#' @param dir A character string of the path to the directory to save files to.
#' @param sep A string of the separator used to construct file names from values.
#'
#' @return An invisible named list of named vectors of the file names and new file names saved.
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
save_all_flobs <- function(table_name = NULL, conn, dir = ".", sep = "_-_"){
  check_sqlite_connection(conn)
  chkor(check_table_name(table_name, conn), chk_null(table_name))
  chk_string(dir)
  chk_string(sep)

  if(is.null(table_name)){
    table_name <- table_names(conn)
  }

  success <- vector(mode = "list")
  success_names <- vector()

  for(i in table_name){
    cols <- blob_columns(i, conn)
    for(j in cols){
      name <- file.path(i, j)
      path <- file.path(dir, name)
      if(!dir.exists(path))
        dir.create(path, recursive = TRUE)
      ui_line(glue("Table name: {ui_value(i)}"))
      ui_line(glue("Column name: {ui_value(j)}"))
      success[[name]] <- save_flobs(j, i, conn, path, sep)
      ui_line("")
    }
  }
  return(invisible(success))
}
