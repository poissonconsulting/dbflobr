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

list_dirs <- function(path, recursive = TRUE, pattern = ".*"){
  dirs <- list.dirs(path, recursive = vld_true(recursive), full.names = TRUE)
  if(pattern != ".*") {
    dirs <- dirs[grepl(pattern, dirs)]
  }
  dirs
}

list_files <- function(path, recursive = TRUE, pattern = ".*"){
  if(pattern == ".*")
    pattern <- NULL

  files <- list.files(path, recursive = !vld_false(recursive), pattern = pattern, full.names = TRUE)
  dirs <- list.dirs(path, recursive = !vld_false(recursive), full.names = TRUE)

  # just those files nested in a subdirectory
  # the file.path(dirname(), basename()) is  hack to get it to work on windows
  if(is.na(recursive)) {
    files <- files[dirname(dirname(files)) == file.path(dirname(path),basename(path))]
  }
  setdiff(files, dirs)
}

dir_tree <- function(path, sub){
  dirs <- setdiff(list.dirs(path, recursive = TRUE, full.names = FALSE), "")
  x <- strsplit(dirs, "/")
  if(!vld_false(sub)) {
    x <- x[which(sapply(x, length) == 2L)]
  } else
    x <- x[which(sapply(x, length) > 1)]
  x
}

is_length_unequal <- function(values, key){
  length(values) > length(key)
}
