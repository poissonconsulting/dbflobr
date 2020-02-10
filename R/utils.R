filename_key <- function(x){
  glue_collapse(x, "-")
}

collapse_flob <- function(x) {
  flobr::chk_flob(x)
  y <- glue_collapse(unlist(x), "")
  glue("x'{y}'")
}

populate_key <- function(key, files){
  if(length(key) != lengths(files))
    err("The number of columns in key and the number of files in dir must be identical.")
}
