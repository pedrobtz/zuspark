#' What zuspark was built with
#'
#' The versions a bug report needs: zuspark's, the Spark tag its protocol
#' definitions come from, and the gRPC and HTTP layers beneath it.
#'
#' @return A `zuspark_info` list: `version`, `spark` (the protocol's Spark
#'   tag), `descriptors` (the SHA-256 of the shipped descriptor set),
#'   `zuproto` and `zugrpc` (the latter's own `zugrpc_info()`).
#' @export
#' @examples
#' zuspark_info()
zuspark_info <- function() {
  pins <- descriptor_version()
  structure(
    list(
      version = as.character(utils::packageVersion("zuspark")),
      spark = pins[["spark"]],
      descriptors = pins[["sha256"]],
      zuproto = as.character(utils::packageVersion("zuproto")),
      zugrpc = zugrpc::zugrpc_info()
    ),
    class = "zuspark_info"
  )
}

#' @export
print.zuspark_info <- function(x, ...) {
  cat(sprintf("zuspark %s, Spark Connect protocol %s\n", x$version, x$spark))
  cat(sprintf("  descriptors: %s\n", substr(x$descriptors, 1L, 16L)))
  cat(sprintf("  zuproto:     %s\n", x$zuproto))
  cat(sprintf(
    "  zugrpc:      %s on zuhttp %s\n",
    x$zugrpc$version,
    x$zugrpc$zuhttp
  ))
  invisible(x)
}
