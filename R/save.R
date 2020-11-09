#' Save flobs.
#'
#' Rename \code{\link[flobr]{flob}}s from a SQLite database BLOB column and save to directory.
#'
#' @inheritParams write_flob
#' @param dir A string of the path to the directory to save the files in.
#' @param sep A string of the separator used to construct file names from values.
#' @param sub A logical specifying whether to save all existing files in a subdirectory
#' of the same name (sub = TRUE) or all possible files in a subdirectory
#' of the same name (sub = NA) or not nest files within a subdirectory (sub = FALSE).
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
save_flobs <- function(column_name, table_name, conn, dir = ".", sep = "_-_",
                       sub = FALSE){
  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_column_name(column_name, table_name, exists = TRUE, conn)
  chk_string(dir)
  chk_string(sep)
  chk_lgl(sub)

  pk <- check_pk(table_name, conn)

  sql <- glue("SELECT {sql_pk(pk)} FROM ('{table_name}');")
  values <- get_query(sql, conn)

  ui_line(glue("Saving files to {ui_value(dir)}"))

  success <- vector()
  success_names <- vector()

  for(i in 1:nrow(values)){
    key <- values[i, , drop = FALSE]
    new_file <- create_filename(key, sep = sep)
    new_file <- as.character(new_file)
    x <- try(read_flob(column_name, table_name, key, conn), silent = TRUE)

     if(!is_try_error(x)){
      filename <- flobr::flob_name(x)
      ext <- flobr::flob_ext(x)
      file <- glue("{filename}.{ext}")
      new_file_ext <- glue("{new_file}.{ext}")
      success[i] <- new_file_ext
      success_names[i] <- file
      if(vld_false(sub)) {
        flobr::unflob(x, dir = dir, name = new_file)
      } else {
        dir.create(file.path(dir, new_file), recursive = TRUE)
        flobr::unflob(x, dir = file.path(dir, new_file), name = new_file)
      }
      ui_done(glue("Row {i}: file {file} renamed to {new_file_ext}"))
    } else {
      if(is.na(sub)) {
        dir.create(file.path(dir, new_file), recursive = TRUE)
      }
      ui_oops(glue("Row {i}: no file found"))
    }
  }
  names(success) <- success_names
  success <- success[!is.na(success)]
  return(invisible(success))
}

#' Save all flobs.
#'
#' Rename \code{\link[flobr]{flob}}s from a SQLite database and save to directory.
#'
#' @inheritParams write_flob
#' @inheritParams save_flobs
#' @param table_name A vector of character strings indicating names of tables to save flobs from.
#' By default all tables are included.
#' @param geometry A flag specifying whether to search columns named geometry for flobs.
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
save_all_flobs <- function(table_name = NULL, conn, dir = ".", sep = "_-_",
                           sub = FALSE,
                           geometry = FALSE){
  check_sqlite_connection(conn)
  chkor(check_table_name(table_name, conn), chk_null(table_name))
  chk_flag(geometry)
  chk_string(dir)
  chk_string(sep)
  chk_lgl(sub)

  if(is.null(table_name)){
    table_name <- table_names(conn)
  }

  success <- vector(mode = "list")
  success_names <- vector()

  for(i in table_name){
    cols <- blob_columns(i, conn)
    if(!geometry) cols <- cols[cols != "geometry"]
    for(j in cols){
      name <- file.path(i, j)
      path <- file.path(dir, name)
      if(!dir.exists(path))
        dir.create(path, recursive = TRUE)
      ui_line(glue("Table name: {ui_value(i)}"))
      ui_line(glue("Column name: {ui_value(j)}"))
      success[[name]] <- save_flobs(j, i, conn, path, sep = sep, sub = sub)
      ui_line("")
    }
  }
  return(invisible(success))
}
