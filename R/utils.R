# Base R gained %||% in 4.4.0; zuspark supports 4.1.
`%||%` <- function(x, y) if (is.null(x)) y else x

# A single string that is not NA, and, unless `empty_ok`, not empty (or
# only blanks).
is_string <- function(x, empty_ok = FALSE) {
  is.character(x) && length(x) == 1L && !is.na(x) && (empty_ok || nzchar(trimws(x)))
}

# A single string, number or logical as the text Spark reads: logicals as
# "true"/"false", numbers in full (100000, not as.character()'s "1e+05").
# NULL for anything else, NA and non-finite numbers included.
scalar_text <- function(v) {
  if (length(v) != 1L || is.na(v)) {
    return(NULL)
  }
  if (is.logical(v)) {
    return(if (v) "true" else "false")
  }
  if (is.numeric(v)) {
    if (!is.finite(v)) {
      return(NULL)
    }
    return(format(v, scientific = FALSE, digits = 15, trim = TRUE))
  }
  if (is.character(v)) {
    return(as.character(unclass(v)))
  }
  NULL
}
