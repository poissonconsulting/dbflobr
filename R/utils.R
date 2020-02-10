filename_key <- function(x){
  glue_collapse(x, "-")
}

collapse_flob <- function(x) {
  flobr::chk_flob(x)
  y <- glue_collapse(unlist(x), "")
  glue("x'{y}'")
}

prep_file <- function(x){
  x <- tools::file_path_sans_ext(x)
  strsplit(x, "-")[[1]]
}

list_files <- function(path, recursive){
  setdiff(list.files(path, recursive = recursive, full.names = TRUE),
          list.dirs(path, recursive = recursive, full.names = TRUE))
}
