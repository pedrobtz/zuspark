# Errors (design §8). zugrpc raises a condition for a non-OK status, a
# transport failure or an interrupt; zuspark turns it into one of its own
# classes, keeping zugrpc's classes after `zuspark_error`, so that code
# matching zugrpc's classes, such as the reattach loop, still sees them.
#
# A failed call carries google.rpc.Status in grpc-status-details-bin, which
# zugrpc decodes; Spark puts one ErrorInfo in it, domain org.apache.spark,
# with metadata keys classes (JSON array), errorClass, errorId,
# messageParameters (JSON object) and sqlState.

as_zuspark_error <- function(e, con = NULL) {
  if (inherits(e, "zuspark_error") || !inherits(e, "zugrpc_error")) {
    return(e)
  }
  info <- e$details$details$ErrorInfo
  status <- e$grpc_status %||% NA_character_
  if (inherits(e, "zugrpc_error_interrupt")) {
    # An R interrupt during a call that is not a stream read (a schema, a
    # setting, connecting): the call was cancelled; only a stream read also
    # asks the server to stop the job (design §7.9, #33).
    cnd_class <- "zuspark_interrupt_error"
    message <- "Interrupted: the call was cancelled."
    fields <- list(interrupted_ids = character(), operation_id = NA_character_)
  } else if (!is.null(info) && identical(info$domain, "org.apache.spark")) {
    meta <- info$metadata %||% character()
    meta_value <- function(key) if (key %in% names(meta)) meta[[key]] else NA_character_
    cnd_class <- "zuspark_sql_error"
    message <- trimws(e$grpc_message %||% e$details$message %||% conditionMessage(e))
    fields <- list(
      error_class = meta_value("errorClass"),
      sql_state = meta_value("sqlState"),
      message_parameters = parse_json_strings(meta_value("messageParameters")),
      classes = parse_json_strings(meta_value("classes")),
      error_id = meta_value("errorId"),
      reason = info$reason %||% NA_character_
    )
  } else {
    cnd_class <- "zuspark_rpc_error"
    message <- conditionMessage(e)
    fields <- list()
  }
  cnd <- errorCondition(
    message,
    class = c(cnd_class, "zuspark_error", setdiff(class(e), c("error", "condition"))),
    call = NULL
  )
  cnd[names(fields)] <- fields
  cnd$grpc_status <- status
  cnd$parent <- e
  # So that spark_error_details(err) can ask the same session.
  if (cnd_class == "zuspark_sql_error") cnd$con <- con
  cnd
}

# Evaluates `expr`, raising any zugrpc condition as zuspark's.
with_zuspark_errors <- function(expr, con = NULL) {
  tryCatch(expr, zugrpc_error = function(e) stop(as_zuspark_error(e, con)))
}

#' Spark's full account of an error
#'
#' A `zuspark_sql_error` carries Spark's error class, SQLSTATE, message
#' parameters and the exception's class hierarchy. `spark_error_details()`
#' asks the server for the rest, through `FetchErrorDetails`: every
#' exception in the cause chain, with its stack trace and the places in the
#' query it points at. The server keeps these only for a while, and only
#' when `spark.sql.connect.enrichError.enabled` is on (the default).
#'
#' @param err A `zuspark_sql_error`, as caught with `tryCatch()`.
#' @param con The connection to ask; by default the one the error came
#'   from.
#' @return A list with one element per exception, the root first. Each is a
#'   list of `type` (the class hierarchy), `message`, `error_class`,
#'   `sql_state`, `message_parameters`, `cause` (the index of its cause in
#'   the list, or `NA`), `stack` (a data frame of `class`, `method`, `file`
#'   and `line`) and `contexts` (a data frame of the query fragments).
#'   When the server no longer holds the details, `spark_error_details()`
#'   raises a `zuspark_error` instead.
#' @export
#' @examplesIf nzchar(Sys.getenv("SPARK_REMOTE"))
#' con <- spark_connect()
#' err <- tryCatch(
#'   spark_sql(con, "SELECT * FROM no_such_table"),
#'   error = identity
#' )
#' err$error_class
#' details <- spark_error_details(err)
#' details[[1]]$message
#' spark_disconnect(con)
spark_error_details <- function(err, con = err$con) {
  if (!inherits(err, "zuspark_sql_error")) {
    zuspark_abort("zuspark_error", "`err` must be a `zuspark_sql_error`.")
  }
  check_open(con)
  if (is.na(err$error_id %||% NA)) {
    zuspark_abort(
      "zuspark_error",
      "This error has no error id: the server keeps details only with `spark.sql.connect.enrichError.enabled` on."
    )
  }
  request <- c(session_fields(con), list(error_id = err$error_id))
  response <- tryCatch(
    spark_rpc(
      con, "FetchErrorDetails", request,
      "spark.connect.FetchErrorDetailsRequest", "spark.connect.FetchErrorDetailsResponse"
    ),
    # Spark 3.5 has no FetchErrorDetails (design §7.2).
    zugrpc_error_unimplemented = function(e) {
      # The same classes, zugrpc's included, with a message that says why
      # (#56).
      stop(errorCondition(
        "This server cannot give error details (it predates Spark 4.0).",
        class = setdiff(class(e), c("error", "condition")),
        call = NULL,
        grpc_status = e$grpc_status,
        parent = e
      ))
    }
  )
  observe_session(con, response)
  errors <- response$errors
  # The server keeps details only for a while, and answers an id it has
  # dropped with no errors at all.
  if (!length(errors)) {
    zuspark_abort(
      "zuspark_error",
      sprintf("The server no longer holds details for error %s.", err$error_id),
      error_id = err$error_id
    )
  }
  root <- response$root_error_idx %||% 0
  error_order <- c(root, setdiff(seq_along(errors) - 1, root))
  lapply(errors[error_order + 1], function(e) error_detail(e, error_order))
}

# `field` of every element of `xs`, as a vector of `type`; `default` where
# an element has none.
pluck <- function(xs, field, default, type) {
  vapply(xs, function(x) as.vector(x[[field]] %||% default, mode = typeof(type)), type)
}

error_detail <- function(e, error_order) {
  throwable <- e$spark_throwable
  stack <- e$stack_trace
  contexts <- throwable$query_contexts
  cause <- e$cause_idx
  list(
    type = unlist(e$error_type_hierarchy) %||% character(),
    message = e$message %||% "",
    error_class = throwable$error_class %||% NA_character_,
    sql_state = throwable$sql_state %||% NA_character_,
    message_parameters = unlist(throwable$message_parameters) %||% character(),
    cause = if (is.null(cause)) NA_integer_ else match(cause, error_order),
    stack = data.frame(
      class = pluck(stack, "declaring_class", "", character(1)),
      method = pluck(stack, "method_name", "", character(1)),
      file = pluck(stack, "file_name", NA_character_, character(1)),
      line = pluck(stack, "line_number", NA_integer_, integer(1)),
      stringsAsFactors = FALSE
    ),
    contexts = data.frame(
      object_type = pluck(contexts, "object_type", "", character(1)),
      object_name = pluck(contexts, "object_name", "", character(1)),
      start = pluck(contexts, "start_index", NA_integer_, integer(1)),
      stop = pluck(contexts, "stop_index", NA_integer_, integer(1)),
      fragment = pluck(contexts, "fragment", "", character(1)),
      stringsAsFactors = FALSE
    )
  )
}

# A flat JSON array of strings, or an object of string values, as Spark
# writes classes and messageParameters: a character vector, named for an
# object. Anything else, or NA, gives character(0).
# The four hex digits after the \u at position `i` of `chars`, as a number,
# or NA when they are cut short or not hex.
json_code_unit <- function(chars, i) {
  hex <- chars[i + 1:4]
  if (anyNA(hex) || !all(grepl("^[0-9A-Fa-f]$", hex))) {
    return(NA_integer_)
  }
  strtoi(paste(hex, collapse = ""), 16L)
}

parse_json_strings <- function(json) {
  if (is.null(json) || is.na(json) || !nzchar(trimws(json))) {
    return(character())
  }
  chars <- strsplit(trimws(json), "")[[1L]]
  n <- length(chars)
  i <- 1L
  strings <- character()
  read_string <- function() {
    out <- character()
    i <<- i + 1L
    while (i <= n && chars[[i]] != "\"") {
      ch <- chars[[i]]
      if (ch == "\\" && i < n) {
        i <<- i + 1L
        esc <- chars[[i]]
        ch <- switch(esc,
          n = "\n", t = "\t", r = "\r", b = "\b", f = "\f",
          u = {
            code <- json_code_unit(chars, i)
            i <<- i + 4L
            # A UTF-16 surrogate pair is one character: \ud83d\ude00.
            if (!is.na(code) && code >= 0xD800 && code <= 0xDBFF &&
              identical(chars[i + 1:2], c("\\", "u"))) {
              low <- json_code_unit(chars, i + 2L)
              if (!is.na(low) && low >= 0xDC00 && low <= 0xDFFF) {
                code <- 0x10000 + (code - 0xD800) * 0x400 + (low - 0xDC00)
                i <<- i + 6L
              }
            }
            # An escape cut short, or a lone surrogate, is the replacement
            # character rather than "NA".
            if (is.na(code) || (code >= 0xD800 && code <= 0xDFFF)) "\ufffd" else intToUtf8(code)
          },
          esc
        )
      }
      out <- c(out, ch)
      i <<- i + 1L
    }
    i <<- i + 1L
    paste(out, collapse = "")
  }
  while (i <= n) {
    if (chars[[i]] == "\"") {
      strings <- c(strings, read_string())
    } else {
      i <- i + 1L
    }
  }
  if (chars[[1L]] == "{") {
    if (length(strings) %% 2L) {
      return(character())
    }
    keys <- strings[c(TRUE, FALSE)]
    return(stats::setNames(strings[c(FALSE, TRUE)], keys))
  }
  strings
}
