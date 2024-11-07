# remove file once `flobr::vld_slob()` is on CRAN
vld_exint <- function(x) {
  vld_s3_class(x, "exint") && vld_scalar(x) && vld_named(x) &&
    vld_s3_class(x[[1]], "integer") && vld_not_any_na(x[[1]])
}

vld_slob <- function(x) {
  if (!(vld_s3_class(x, "blob") && vld_scalar(x) && vld_list(x))) {
    return(FALSE)
  }

  exint <- unlist(x)
  exint <- try(unserialize(exint), silent = TRUE)
  if (inherits(exint, "try-error")) {
    return(FALSE)
  }

  class(exint) <- "exint"
  vld_exint(exint)
}
