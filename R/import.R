#' Import flobs.
#'
#' Import \code{\link[flobr]{flob}}s to SQLite database column from directory.
#' Values in file name are matched to table primary key to determine where to write flob.
#'
#' @inheritParams write_flob
#' @param dir A string of the path to the directory to import files from.
#' @param sep A string of the separator between values in file names.
#' @param recursive A flag indicating whether to recurse into file directory (TRUE) or not (FALSE).
#' @param replace A flag indicating whether to replace existing flobs (TRUE) or not (FALSE).
#' @param pattern A regular expression specifying the pattern file names must match.
#' @param sub A logical scalar specifying whether to import flobs based on their filename (sub = FALSE)
#' or the name of their subdirectory (sub = TRUE) which must only contain 1 file.
#' If sub = NA and replace = TRUE then the names of the
#' subdirectories are used irrespective of whether they include files and existing
#' flobs are deleted if the corresponding subdirectory is empty.
#' If sub = TRUE or sub = NA then recursion is just one subfolder deep.
#' @return An invisible named vector indicating file name and whether the file was
#' successfully written to database.
#' @export
#' @examples
#' conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' DBI::dbGetQuery(conn, "CREATE TABLE Table1 (CharColumn TEXT PRIMARY KEY NOT NULL)")
#' DBI::dbWriteTable(conn, "Table1", data.frame(CharColumn = c("a", "b")), append = TRUE)
#' key <- data.frame(CharColumn = "a", stringsAsFactors = FALSE)[0,,drop = FALSE]
#' dir <- tempdir()
#' write.csv(key, file.path(dir, "a.csv"))
#' import_flobs("BlobColumn", "Table1", conn, dir)
#' DBI::dbDisconnect(conn)
import_flobs <- function(column_name, table_name, conn,
                         dir = ".", sep = "_-_", pattern = ".*", sub = FALSE,
                         exists = FALSE, recursive = FALSE,
                         replace = FALSE){
  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_column_name(column_name, table_name, exists = exists, conn)
  chk_dir(dir)
  chk_string(sep)
  chk_flag(exists)
  chk_flag(recursive)
  chk_flag(replace)
  check_pk(table_name, conn)
  chk_lgl(sub)
  chk_string(pattern)


  if(vld_false(sub)) {
    files <- list_files(dir, recursive = recursive, pattern = pattern)
    names(files) <- basename(files)
    if(anyDuplicated(names(files)))
      stop("File names must be unique.", call. = FALSE)
  } else {
    files <- list_files(dir, recursive = NA, pattern = pattern)
    names(files) <- basename(dirname(files))
    if(anyDuplicated(names(files)))
      stop("Directory names must be unique.", call. = FALSE)
    if(is.na(sub)) {
      dirs <- list_dirs(dir, recursive = NA, pattern = pattern)
      names(dirs) <- basename(dirs)
      dirs[names(files)] <- files
      dirs[!names(dirs) %in% names(files)] <- NA_character_
      files <- dirs
    }
  }

  key <- table_pk_df(table_name, conn)

  column_exists <- column_exists(column_name, table_name, conn = conn)
  if(!exists && !column_exists)
    add_blob_column(column_name, table_name, conn)

  ui_line(glue("Writing files to database"))

  success <- rep(FALSE, length = length(files))
  names(success) <- names(files)

  for(i in seq_along(files)){
    values <- parse_filename(names(files)[i], sep)

    if(is_length_unequal(values, key)){
      ui_oops(glue("File {i}: can't write {names(files)[i]} to database. The number of hyphen-separated values must be identical to the number of columns in `key`."))
      next
    }

    for(j in seq_along(values)){
      key[i, j] <- values[j]
    }

    y <- try(read_flob(column_name, table_name, key[i,, drop = FALSE], conn), silent = TRUE)
    if(!replace && !is_try_error(y)){
      ui_oops(glue("File {i}: can't write {names(files)[i]} to database. Flob already exists in that location and replace = FALSE"))
      next
    }

    if(!is.na(files[i])) {
      flob <- flobr::flob(files[i])
      x <- try(write_flob(flob, key = key[i,,drop = FALSE],
                          column_name = column_name,
                          table_name = table_name,
                          conn = conn,
                          exists = TRUE), silent = TRUE)

      if(!is_try_error(x)){
        success[i] <- TRUE
        ui_done(glue("File {i}: {names(files)[i]} written to database"))
      } else {
        ui_oops(glue("File {i}: can't write {names(files)[i]} to database"))
      }
      next
    }

    if(is_try_error(y)) {
      ui_done(glue("File {i}: {names(files)[i]} already absent from database."))
      success[i] <- TRUE
      next
    }
    x <- try(delete_flob(key = key[i,,drop = FALSE],
                         column_name = column_name,
                         table_name = table_name,
                         conn = conn))
    if(!is_try_error(x)){
      success[i] <- TRUE
      ui_done(glue("File {i}: {names(files)[i]} deleted in database."))
    } else {
      ui_oops(glue("File {i}: can't delete {names(files)[i]} in database."))
    }
  }
  return(invisible(success))
}

#' Import all flobs.
#'
#' Import \code{\link[flobr]{flob}}s to SQLite database from directory.
#' Table and column names are matched to directory names within main directory.
#' Values in file names are matched to table primary key to determine where to write flob.
#'
#' @inheritParams write_flob
#' @inheritParams import_flobs
#' @param dir A string of the path to the directory to import the files from.
#' Files need to be within nested folders like 'table1/column1/a.csv'.
#' This structure is created automatically if save_all_flobs() function is used.
#' @return An invisible named list indicating directory path,
#' file names and whether files were successfully written to database.
#' @export
#' @examples
#' conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' DBI::dbGetQuery(conn, "CREATE TABLE Table1 (CharColumn TEXT PRIMARY KEY NOT NULL)")
#' DBI::dbWriteTable(conn, "Table1", data.frame(CharColumn = c("a", "b")), append = TRUE)
#' flob <- flobr::flob_obj
#' write_flob(flob, "BlobColumn", "Table1", data.frame(CharColumn = "a"), conn)
#' dir <- file.path(tempdir(), "import_all")
#' save_all_flobs(conn = conn, dir = dir)
#' import_all_flobs(conn, dir, exists = TRUE, replace = TRUE)
#' DBI::dbDisconnect(conn)
import_all_flobs <- function(conn, dir = ".", sep = "_-_", pattern = ".*",
                             sub = FALSE,
                             exists = FALSE, replace = FALSE){
  check_sqlite_connection(conn)
  chk_dir(dir)
  chk_string(sep)
  chk_flag(exists)
  chk_flag(replace)
  chk_string(pattern)
  chk_lgl(sub)

  dirs <- dir_tree(dir, sub)
  success <- vector(mode = "list", length = length(dirs))

  for(i in seq_along(dirs)){
    x <- dirs[[i]]
    table_name <- x[1]
    column_name <- x[2]
    inner_dir <- file.path(dir, table_name, column_name)
    ui_line(glue("Table name: {ui_value(table_name)}"))
    ui_line(glue("Column name: {ui_value(column_name)}"))
    success[[i]] <- import_flobs(column_name = x[2], table_name = x[1],
                                 conn = conn, dir = inner_dir, sep = sep,
                                 pattern = pattern, sub = sub,
                                 exists = exists, replace = replace)
    ui_line("")
  }

  names(success) <- sapply(dirs, glue_collapse, "/")
  return(invisible(success))
}
