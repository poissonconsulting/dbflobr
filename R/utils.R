create_filename <- function(x, sep){
  glue_collapse(x, sep)
}

parse_filename <- function(x, sep){
  x <- tools::file_path_sans_ext(x)
  strsplit(x, sep)[[1]]
}

collapse_flob <- function(x) {
  chk_flob(x)
  y <- glue_collapse(unlist(x), "")
  glue("x'{y}'")
}

list_files <- function(path, recursive = TRUE){
  setdiff(list.files(path, recursive = recursive, full.names = TRUE),
          list.dirs(path, recursive = recursive, full.names = TRUE))
}

dir_tree <- function(path){
  dirs <- setdiff(list.dirs(path, recursive = TRUE, full.names = FALSE), "")
  x <- strsplit(dirs, "/")
  x[which(sapply(x, length) > 1)]
}

is_length_unequal <- function(values, key){
  length(values) > length(key)
}
