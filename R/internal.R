.quotes <- "^(`|[[]|\")(.*)(`|[]]|\")$"

is_quoted <- function(x) grepl(.quotes, x)

to_upper <- function(x) {
  x <- as.character(x)
  is_quoted <- is_quoted(x)
  x[!is_quoted] <- toupper(x[!is_quoted])
  x
}

collapse_flob <- function(x) {
  flobr::check_flob(x)
  y <- glue_collapse(unlist(x), "")
  glue("x'{y}'")
}

err <- function (...) stop(..., call. = FALSE, domain = NA)
