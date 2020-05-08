.quotes <- "^(`|[[]|\")(.*)(`|[]]|\")$"

is_quoted <- function(x) grepl(.quotes, x)

to_upper <- function(x) {
  x <- as.character(x)
  is_quoted <- is_quoted(x)
  x[!is_quoted] <- toupper(x[!is_quoted])
  x
}

is_try_error <- function(x) {
  inherits(x, "try-error")
}

set_names <- function(x, names) {
  names(x) <- names
  x
}
