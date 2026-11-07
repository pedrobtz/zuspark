#' Read and change the session's configuration
#'
#' One function for the session's Spark SQL configuration, through Spark
#' Connect's `Config` call. With no arguments it returns every setting.
#' Unnamed strings name settings to read. Named arguments set settings, and
#' a named `NULL` or `NA` unsets one, returning to the server's default.
#' The server's warnings, such as for a deprecated setting, become R
#' warnings.
#'
#' @inheritParams spark_disconnect
#' @param ... Setting names to read, or `name = value` pairs to set. A
#'   logical value is sent as `"true"` or `"false"`.
#'
#' @return Reading, a named character vector of values, `NA` for a setting
#'   that has none. Setting, the previous values invisibly, in the same
#'   form, so that passing them back restores the session:
#'   `old <- spark_config(con, a = "1")`, then
#'   `do.call(spark_config, c(list(con), as.list(old)))`.
#' @export
#' @examplesIf nzchar(Sys.getenv("SPARK_REMOTE"))
#' con <- spark_connect()
#' spark_config(con, "spark.sql.session.timeZone")
#' old <- spark_config(con, spark.sql.session.timeZone = "UTC")
#' do.call(spark_config, c(list(con), as.list(old)))
#' spark_disconnect(con)
spark_config <- function(con, ...) {
  check_open(con)
  args <- list(...)
  if (!length(args)) {
    return(config_rpc(con, list(get_all = list())))
  }

  keys <- names(args)
  if (is.null(keys) || all(!nzchar(keys))) {
    keys <- unlist(args, use.names = FALSE)
    if (!is.character(keys) || anyNA(keys) || any(!nzchar(keys))) {
      zuspark_abort(
        "zuspark_error",
        "Settings to read must be non-empty strings."
      )
    }
    return(config_rpc(con, list(get_option = list(keys = keys))))
  }
  if (any(!nzchar(keys))) {
    zuspark_abort(
      "zuspark_error",
      "Pass either setting names to read or `name = value` pairs to set, not both."
    )
  }

  unset <- vapply(
    args,
    function(v) is.null(v) || (length(v) == 1L && is.na(v)),
    logical(1)
  )
  values <- vapply(args[!unset], config_value, character(1))

  old <- config_rpc(con, list(get_option = list(keys = keys)))
  if (length(values)) {
    pairs <- Map(
      function(k, v) list(key = k, value = v),
      names(values),
      unname(values)
    )
    config_rpc(con, list(set = list(pairs = unname(pairs))))
  }
  if (any(unset)) {
    config_rpc(con, list(unset = list(keys = keys[unset])))
  }
  invisible(old)
}

config_value <- function(v) {
  text <- scalar_text(v)
  if (is.null(text)) {
    zuspark_abort(
      "zuspark_error",
      "Each setting's value must be a single string, number or logical."
    )
  }
  text
}

config_rpc <- function(con, operation) {
  request <- c(session_fields(con), list(operation = operation))
  response <- spark_rpc(
    con,
    "Config",
    request,
    "spark.connect.ConfigRequest",
    "spark.connect.ConfigResponse"
  )
  observe_session(con, response)
  for (w in response$warnings) {
    warning(w, call. = FALSE)
  }
  config_pairs(response)
}

# A ConfigResponse's pairs as a named character vector; a key without a
# value is NA.
config_pairs <- function(response) {
  pairs <- response$pairs
  keys <- vapply(pairs, function(p) p$key %||% NA_character_, character(1))
  if (anyNA(keys)) {
    zuspark_abort(
      "zuspark_protocol_error",
      "The server sent a setting without a name."
    )
  }
  values <- vapply(pairs, function(p) p$value %||% NA_character_, character(1))
  stats::setNames(values, keys)
}
