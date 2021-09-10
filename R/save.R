#' Save flobs.
#'
#' Rename \code{\link[flobr]{flob}}s from a SQLite database BLOB column and save to directory.
#'
#' @inheritParams write_flob
#' @param dir A string of the path to the directory to save the files in.
#' @param sep A string of the separator used to construct file names from values.
#' @param sub A logical scalar specifying whether to save all existing files in a subdirectory
#' of the same name (sub = TRUE) or all possible files in a subdirectory
#' of the same name (sub = NA) or not nest files within a subdirectory (sub = FALSE).
#' @param replace A flag specifying whether to replace existing files.
#' If sub = TRUE (or sub = NA) and replace = TRUE then all existing files
#' within a subdirectory are deleted.
#' @param slob_ext A string of the file extension to use if slobs (serialized blobs) are encountered.
#' If slob_ext = NULL slobs will be ignored.
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
save_flobs <- function(column_name, table_name, conn, dir = ".", sep = "_-_", sub = FALSE, replace = FALSE, slob_ext = NULL){
  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_column_name(column_name, table_name, exists = TRUE, conn)
  chk_string(dir)
  chk_string(sep)
  chk_lgl(sub)
  chk_flag(replace)

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
    x <- try(read_flob(column_name, table_name, key, conn, slob = NA), silent = TRUE)
    if(!is_try_error(x) && !(blob::is_blob(x) && is.null(slob_ext))){
      if(flobr::is_flob(x) || is.null(slob_ext)){
        filename <- flobr::flob_name(x)
        ext <- flobr::flob_ext(x)
        file <- glue("{filename}.{ext}")
        new_file_ext <- glue("{new_file}.{ext}")
        success[i] <- new_file_ext
        success_names[i] <- file
      } else {
        if(is.null(slob_ext)) err("`slob_ext` must be provided when slobs are present.")
        filename <- "BLOB"
        ext <- slob_ext
        file <- glue("{filename}")
        new_file_ext <- glue("{new_file}.{ext}")
        success[i] <- new_file_ext
        success_names[i] <- file
      }

      if(vld_false(sub)) {
        if(!replace && file.exists(file.path(dir, new_file_ext))) {
          stop("File '", file.path(dir, new_file), "' already exists.", call. = FALSE)
        }

        flobr::unflob(x, dir = dir, name = new_file, ext = ext, slob = NA, check = FALSE)
      } else {
        if(!replace && length(list.files(file.path(dir, new_file)))) {
          stop("Directory '", file.path(dir, new_file), "' already contains a file.", call. = FALSE)
        }
        unlink(file.path(dir, new_file), recursive = TRUE)
        dir.create(file.path(dir, new_file), recursive = TRUE)
        flobr::unflob(x, dir = file.path(dir, new_file), name = new_file, ext = ext, slob = NA, check = FALSE)
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
#' @inheritParams save_flobs
#' @inheritParams write_flob
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
                           sub = FALSE, replace = FALSE,
                           geometry = FALSE){
  check_sqlite_connection(conn)
  if(!is.null(table_name)) {
    check_table_name(table_name, conn)
}
  chk_flag(geometry)
  chk_string(dir)
  chk_string(sep)
  chk_lgl(sub)
  chk_flag(replace)

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
      success[[name]] <- save_flobs(j, i, conn, path, sep = sep, sub = sub,
                                    replace = replace)
      ui_line("")
    }
  }
  return(invisible(success))
}
