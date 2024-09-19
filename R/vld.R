vld_exint <- function(x) {
  vld_s3_class(x, "exint") && vld_scalar(x) && vld_named(x) &&
    vld_s3_class(x[[1]], "integer") && vld_not_any_na(x[[1]])
}

    return(FALSE)
  }

  exint <- unlist(x)
  exint <- try(unserialize(exint), silent = TRUE)
  if (inherits(exint, "try-error")) {
    return(FALSE)
  }

  vld_exint(exint)
}
