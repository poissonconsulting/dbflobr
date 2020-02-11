#' Import flobs.
#'
#' Import flobs to SQLite database column from directory.
#'
#' @inheritParams write_flob
#' @param key An empty data.frame whose columns and types are populated by
#' file names in dir. The populated key is used to filter the table to a
#' single row (this in combination with the column_name argument are used to
#' target a single cell within the table to modify).
#' @param dir A string of the path to the directory to save the files in.
#' @param recursive A flag indicating whether to recurse into file directory.
#' @param replace A flag indicating whether to replace existing flobs.
#'
#' @return An invisible named vector of file names and flag indicating whether
#' successfully written to database.
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
import_flobs <- function(column_name, table_name, key, conn,
                         dir = ".", exists = FALSE, recursive = FALSE,
                         replace = TRUE){
  check_sqlite_connection(conn)
  check_table_name(table_name, conn)
  check_column_name(column_name, table_name, exists = exists, conn)
  check_string(dir)
  check_inherits(key, "data.frame")
  check_nrow(key, 0L)
  check_flag(exists)
  check_flag(recursive)
  check_pk_key(table_name, conn, key)

  files <- list_files(dir, recursive = recursive)
  filenames <- basename(files)

  check_nrows(table_name, conn, files)

  column_exists <- column_exists(column_name, table_name, conn = conn)
  if(!exists && !column_exists)
    add_blob_column(column_name, table_name, conn)

  ui_line(glue("Writing files to connection"))

  success <- set_names(vector(length = length(files)), filenames)

  for(i in seq_along(files)){
    values <- prep_file(filenames[i])
    flob <- flobr::flob(files[i])

    if(is_length_unequal(values, key)){
      ui_todo(glue("File {i}: can't write {filenames[i]} to database. The number of hyphen-separated values must be identical to the number of columns in `key`."))
      next
    }

    for(j in seq_along(values)){
      key[i, j] <- values[j]
    }

    y <- try(read_flob(column_name, table_name, key[i,], conn), silent = TRUE)
    if(!replace && !is_try_error(y)){
      ui_todo(glue("File {i}: can't write {filenames[i]} to database. Flob already exists in that location and replace = FALSE"))
      next
    }

    x <- try(write_flob(flob, key = key[i,],
                        column_name = column_name,
                        table_name = table_name,
                        conn = conn,
                        exists = TRUE), silent = TRUE)

    if(!is_try_error(x)){
      success[i] <- TRUE
      ui_done(glue("File {i}: {filenames[i]} written to database"))
    } else {
      ui_todo(glue("File {i}: can't write {filenames[i]} to database"))
    }
  }
  return(invisible(success))
}

#' #' Import all flobs.
#' #'
#' #' Import flobs to SQLite database from directory.
#' #'
#' #' @inheritParams write_flob
#' #' @param key An empty data.frame whose columns and types are populated by
#' #' file names in dir. The populated key is used to filter the table to a
#' #' single row (this in combination with the column_name argument are used to
#' #' target a single cell within the table to modify).
#' #' @param dir A string of the path to the directory to save the files in.
#' #' @param recursive A flag indicating whether to recurse into file directory.
#' #' @param replace A flag indicating whether to replace existing flobs.
#' #'
#' #' @return An invisible named vector of file names and flag indicating whether
#' #' successfully written to database.
#' #' @export
#' #' @examples
#' #' flob <- flobr::flob_obj
#' #' conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' #' DBI::dbGetQuery(conn, "CREATE TABLE Table1 (IntColumn INTEGER PRIMARY KEY NOT NULL)")
#' #' DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)), append = TRUE)
#' #' key <- data.frame(IntColumn = 2L)
#' #' write_flob(flob, "BlobColumn", "Table1", key, conn, exists = FALSE)
#' #' dir <- tempdir()
#' #' save_flobs("BlobColumn", "Table1", conn, dir)
#' #' DBI::dbDisconnect(conn)
#' import_flobs <- function(column_name, table_name, key, conn,
#'                          dir = ".", exists = FALSE, recursive = FALSE,
#'                          replace = TRUE){
#'   check_sqlite_connection(conn)
#'   check_table_name(table_name, conn)
#'   check_column_name(column_name, table_name, exists = exists, conn)
#'   check_string(dir)
#'   check_inherits(key, "data.frame")
#'   check_nrow(key, 0L)
#'   check_flag(exists)
#'   check_flag(recursive)
#'   check_pk_key(table_name, conn, key)
#'
#'   files <- list_files(dir, recursive = recursive)
#'   filenames <- basename(files)
#'
#'   column_exists <- column_exists(column_name, table_name, conn = conn)
#'   if(!exists && !column_exists)
#'     add_blob_column(column_name, table_name, conn)
#'
#'   ui_line(glue("Writing files to connection"))
#'
#'   success <- set_names(vector(length = length(files)), filenames)
#'
#'   for(i in seq_along(files)){
#'     values <- prep_file(filenames[i])
#'     flob <- flobr::flob(files[i])
#'
#'     if(is_length_unequal(values, key)){
#'       ui_todo(glue("File {i}: can't write {filenames[i]} to database. The number of hyphen-separated values must be identical to the number of columns in `key`."))
#'       next
#'     }
#'
#'     for(j in seq_along(values)){
#'       key[i, j] <- values[j]
#'     }
#'
#'     y <- try(read_flob(column_name, table_name, key[i,], conn), silent = TRUE)
#'     if(!replace && !is_try_error(y)){
#'       ui_todo(glue("File {i}: can't write {filenames[i]} to database. Flob already exists in that location and replace = FALSE"))
#'       next
#'     }
#'
#'     x <- try(write_flob(flob, key = key[i,],
#'                         column_name = column_name,
#'                         table_name = table_name,
#'                         conn = conn,
#'                         exists = TRUE), silent = TRUE)
#'
#'     if(!is_try_error(x)){
#'       success[i] <- TRUE
#'       ui_done(glue("File {i}: {filenames[i]} written to database"))
#'     } else {
#'       ui_todo(glue("File {i}: can't write {filenames[i]} to database"))
#'     }
#'   }
#'   return(invisible(success))
#' }
#'
