#' Save flobs.
#'
#' Save and rename \code{\link[flobr]{flob}}s from a SQLite database table and column to directory.
#'
#' @inheritParams write_flob
#' @param dir A character string of the path to the directory to save files to.
#'
#' @return An invisible list of the original file names.
#' @export
#' @examples
#' flob <- flobr::flob(system.file("extdata", "flobr.pdf", package = "flobr"))
#' conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' DBI::dbWriteTable(conn, "Table1", data.frame(IntColumn = c(1L, 2L)))
#' key <- data.frame(IntColumn = 2L)
#' write_flob(flob, "BlobColumn", "Table1", key, conn, exists = FALSE)
#' dir <- tempdir()
#' save_flobs("BlobColumn", "Table1", dir, conn)
#' DBI::dbDisconnect(conn)
save_flobs <- function(column_name, table_name, dir, conn){

  pk <- table_pk(table_name, conn)
  sql <- glue("SELECT {sql_pk(pk)} FROM ('{table_name}');")
  values <- get_query(sql, conn)

  usethis::ui_line(glue("Saving files to {dir} ..."))

  for(i in 1:nrow(values)){
    key <- values[i, , drop = FALSE]
    x <- try(read_flob(column_name, table_name, key, conn), silent = TRUE)

     if(!is_try_error(x)){
      filename <- flobr::flob_name(x)
      ext <- flobr::flob_ext(x)
      file <- glue("{filename}.{ext}")
      new_file <- filename_key(key)
      flobr::unflob(x, dir = dir, name = new_file)
      usethis::ui_done(glue("Row {i}: file {file} renamed to {new_file}.{ext}"))
    } else {
      usethis::ui_todo(glue("Row {i}: no file found"))
    }
  }
}

# messages
### moving files to directory
# file blah renamed to blah

### save_all_flobs create nested dir of table names and columns names with files in it
## tabke_name arg deafulat all tables, but user can provide vector of table names

