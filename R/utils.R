# Base R gained %||% in 4.4.0; zuspark supports 4.1.
`%||%` <- function(x, y) if (is.null(x)) y else x
