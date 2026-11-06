# Sessions (design §7.2). A connection is an environment, so that the
# server-side session id learnt from the first response, and the released
# state, are shared by every copy of the object.

#' Connect to a Spark Connect server
#'
#' `spark_connect()` opens a session on a Spark Connect endpoint: it builds
#' the gRPC channel the connection string describes, proves it with one
#' `Config` call and one `SparkVersion` call, and refuses servers older than
#' Spark 3.5.0. `spark_disconnect()` releases the session on the server and
#' closes the channel.
#'
#' @param url A connection string, `sc://host:port/;param=value;...`. It
#'   defaults to the `SPARK_REMOTE` environment variable. The default port
#'   is 15002. The reserved parameters are `token` (a bearer token; it
#'   turns TLS on, except to `localhost` without `use_ssl`), `use_ssl`,
#'   `user_id`, `user_agent`, `session_id`, `grpc_max_message_size` and
#'   `grpc_keepalive_enabled`, `_time_ms` and `_timeout_ms`. Every other
#'   parameter is sent as a header on every call.
#' @param ... Named parameters appended to `url`, such as `token = "..."`.
#' @param user_id,session_id,user_agent The user id sent with every call
#'   (by default the `user_id` parameter, then `SPARK_USER`, then `USER`),
#'   the session's id (a UUID; by default a new version 4 UUID), and the
#'   text that starts the client type the server logs (by default
#'   `SPARK_CONNECT_USER_AGENT`, then `"zuspark"`). Each takes precedence
#'   over the parameter of the same name in `url`.
#' @param headers Extra headers for every call, as a named character
#'   vector, for names that are not valid in a URL.
#' @param deadline Seconds each call other than a query may take. The
#'   default is 30.
#'
#' @return `spark_connect()` returns a `zuspark_connection`.
#'   `spark_disconnect()` returns `TRUE` invisibly, or `FALSE` if the
#'   session was already released.
#' @export
#' @examplesIf nzchar(Sys.getenv("SPARK_REMOTE"))
#' con <- spark_connect()
#' con
#' spark_disconnect(con)
spark_connect <- function(url = Sys.getenv("SPARK_REMOTE"), ...,
                          user_id = NULL, session_id = NULL,
                          user_agent = NULL, headers = NULL,
                          deadline = NULL) {
  if (!is.character(url) || length(url) != 1L || is.na(url) || !nzchar(url)) {
    url_abort(
      "url",
      "No connection string: pass `url`, or set SPARK_REMOTE, such as 'sc://localhost:15002'."
    )
  }
  check_text_arg(user_id, "user_id")
  check_text_arg(user_agent, "user_agent")
  check_deadline(deadline)
  url <- append_url_params(url, list(...))
  u <- parse_spark_url(url)

  user_id <- user_id %||% u$user_id %||% default_user_id()
  session_id <- session_id %||% u$session_id %||% new_uuid()
  check_session_id(session_id)
  # A classed id, such as zuid's uuid, would never be identical() to the
  # plain string the server answers with.
  session_id <- as.character(unclass(session_id))

  metadata <- connection_metadata(u, headers)

  con <- new.env(parent = emptyenv())
  con$url <- redact_url(url)
  con$endpoint <- u$url
  con$session_id <- session_id
  con$server_side_session_id <- NULL
  con$user_id <- user_id
  con$client_type <- client_type(user_agent %||% u$user_agent)
  con$deadline <- deadline %||% 30
  con$transport <- getOption("zuspark.transport", grpc_transport)
  # The channel holds the bearer token in its headers, so it is kept in the
  # namespace's registry, not on the connection: a query or an error that
  # is saved or sent to another process carries the connection, and with
  # it only the key (#26).
  con$channel_key <- new_uuid()
  channels[[con$channel_key]] <- zugrpc::grpc_channel(
    u$url,
    headers = if (length(metadata)) metadata,
    keepalive = u$keepalive,
    max_message = u$max_message
  )
  con$released <- FALSE
  class(con) <- "zuspark_connection"
  reg.finalizer(con, finalize_connection)

  tryCatch(
    {
      config_rpc(con, list(get_all = list()))
      con$spark_version <- version_rpc(con)
    },
    error = function(e) {
      close_connection(con)
      stop(e)
    }
  )
  check_server_version(con)
  con
}

#' @rdname spark_connect
#' @param con A `zuspark_connection`.
#' @param allow_reconnect If `TRUE`, the server lets a later connection
#'   reuse this session's id. By default the id is retired with the
#'   session.
#' @export
spark_disconnect <- function(con, allow_reconnect = FALSE) {
  check_connection(con)
  if (isTRUE(con$released)) {
    return(invisible(FALSE))
  }
  on.exit(close_connection(con))
  release_session(con, allow_reconnect = isTRUE(allow_reconnect))
  invisible(TRUE)
}

#' Inspect a session
#'
#' `spark_version()` is the Spark version the server reported when the
#' session was opened. `spark_session_id()` gives the session's two ids:
#' the client's, sent with every call, and the server's, which the server
#' returns on every response and which changes if the server restarts.
#'
#' @inheritParams spark_disconnect
#' @return `spark_version()` returns a string such as `"4.2.1"`.
#'   `spark_session_id()` returns a character vector with names `client`
#'   and `server`.
#' @export
#' @examplesIf nzchar(Sys.getenv("SPARK_REMOTE"))
#' con <- spark_connect()
#' spark_version(con)
#' spark_session_id(con)
#' spark_disconnect(con)
spark_version <- function(con) {
  check_connection(con)
  con$spark_version
}

#' @rdname spark_version
#' @export
spark_session_id <- function(con) {
  check_connection(con)
  c(client = con$session_id, server = con$server_side_session_id %||% NA_character_)
}

#' @export
print.zuspark_connection <- function(x, ...) {
  state <- if (isTRUE(x$released)) "released" else paste("Spark", x$spark_version %||% "?")
  cat(sprintf("<zuspark_connection> %s (%s)\n", x$url, state))
  cat(sprintf("  session: %s\n", x$session_id))
  invisible(x)
}

# The fields every request carries. ReleaseSession has no field for the
# observed server-side id, so `observed = FALSE` leaves it out.
session_fields <- function(con, observed = TRUE) {
  check_open(con)
  fields <- list(
    session_id = con$session_id,
    user_context = list(user_id = con$user_id),
    client_type = con$client_type
  )
  if (observed && !is.null(con$server_side_session_id)) {
    fields$client_observed_server_side_session_id <- con$server_side_session_id
  }
  fields
}

# Every response repeats both ids. The client's must be ours; the server's
# is learnt from the first response, and a different one later means the
# server lost the session (a restart that reused the client id).
observe_session <- function(con, response) {
  sid <- response$session_id
  if (!is.null(sid) && nzchar(sid) && !identical(sid, con$session_id)) {
    zuspark_abort(
      "zuspark_session_error",
      sprintf("The server answered for session %s, not %s.", sid, con$session_id)
    )
  }
  server <- response$server_side_session_id
  if (is.null(server) || !nzchar(server)) {
    return(invisible())
  }
  if (is.null(con$server_side_session_id)) {
    con$server_side_session_id <- server
  } else if (!identical(server, con$server_side_session_id)) {
    zuspark_abort(
      "zuspark_session_error",
      sprintf(
        "The server-side session changed from %s to %s: the server lost this session. Reconnect with `spark_connect()`.",
        con$server_side_session_id, server
      )
    )
  }
  invisible()
}

version_rpc <- function(con) {
  request <- c(session_fields(con), list(spark_version = list()))
  response <- spark_rpc(
    con, "AnalyzePlan", request,
    "spark.connect.AnalyzePlanRequest", "spark.connect.AnalyzePlanResponse"
  )
  observe_session(con, response)
  response$spark_version$version %||% ""
}

check_server_version <- function(con) {
  version <- con$spark_version
  number <- numeric_version(sub("[^0-9.].*$", "", version), strict = FALSE)
  if (is.na(number) || number < "3.5.0") {
    close_connection(con)
    zuspark_abort(
      "zuspark_version_error",
      sprintf(
        "The server runs Spark %s; zuspark needs Spark 3.5.0 or later.",
        if (nzchar(version)) version else "of an unknown version"
      ),
      version = version
    )
  }
  invisible()
}

release_session <- function(con, allow_reconnect = FALSE, deadline = con$deadline) {
  request <- c(session_fields(con, observed = FALSE), list(allow_reconnect = allow_reconnect))
  tryCatch(
    {
      response <- spark_rpc(
        con, "ReleaseSession", request,
        "spark.connect.ReleaseSessionRequest", "spark.connect.ReleaseSessionResponse",
        deadline = deadline
      )
      observe_session(con, response)
    },
    # Spark 3.5 has no ReleaseSession: the server drops the idle session
    # itself, so this is "not available", not a failure.
    zugrpc_error_unimplemented = function(e) NULL
  )
  invisible()
}

close_connection <- function(con) {
  con$released <- TRUE
  key <- con$channel_key
  if (!is.null(key) && exists(key, envir = channels, inherits = FALSE)) {
    try(zugrpc::grpc_close(channels[[key]]), silent = TRUE)
    rm(list = key, envir = channels)
  }
  invisible()
}

# The connections' channels, by key; see spark_connect().
channels <- new.env(parent = emptyenv())

connection_channel <- function(con) {
  key <- con$channel_key
  if (is.null(key) || !exists(key, envir = channels, inherits = FALSE)) {
    zuspark_abort(
      "zuspark_session_error",
      "This connection has no channel in this R session: it was closed, or made in another session. Reconnect with `spark_connect()`."
    )
  }
  channels[[key]]
}

# Best effort, as PySpark's: a short deadline, and never an error from a
# finalizer.
finalize_connection <- function(con) {
  if (!isTRUE(con$released)) {
    try(release_session(con, deadline = 5), silent = TRUE)
    close_connection(con)
  }
  invisible()
}

check_connection <- function(con) {
  if (!inherits(con, "zuspark_connection")) {
    zuspark_abort(
      "zuspark_error",
      "`con` must be a connection from `spark_connect()`."
    )
  }
  invisible(con)
}

check_open <- function(con) {
  check_connection(con)
  if (isTRUE(con$released)) {
    zuspark_abort(
      "zuspark_session_error",
      "This session was released by `spark_disconnect()`. Reconnect with `spark_connect()`."
    )
  }
  invisible(con)
}

# PySpark's client type: the user agent, then the client, protocol, system
# and language versions, space-separated. Only the user agent is limited,
# to 2048 characters once percent-encoded.
client_type <- function(user_agent = NULL) {
  if (is.null(user_agent)) {
    user_agent <- Sys.getenv("SPARK_CONNECT_USER_AGENT", "zuspark")
  }
  encoded <- nchar(utils::URLencode(user_agent, reserved = TRUE))
  if (encoded > 2048L) {
    zuspark_abort(
      "zuspark_url_error",
      sprintf(
        "'user_agent' must not exceed 2048 characters after URL escaping; it has %d.",
        encoded
      ),
      part = "user_agent"
    )
  }
  paste(
    user_agent,
    paste0("zuspark/", utils::packageVersion("zuspark")),
    paste0("spark/", sub("^v", "", descriptor_version()[["spark"]])),
    paste0("os/", tolower(Sys.info()[["sysname"]])),
    paste0("r/", getRversion())
  )
}

default_user_id <- function() {
  for (var in c("SPARK_USER", "USER")) {
    value <- Sys.getenv(var)
    if (nzchar(value)) {
      return(value)
    }
  }
  # Windows sets neither.
  Sys.info()[["user"]]
}

new_uuid <- function() {
  as.character(zuid::uuid(version = 4))
}

append_url_params <- function(url, params) {
  if (!length(params)) {
    return(url)
  }
  keys <- names(params)
  if (is.null(keys) || any(!nzchar(keys))) {
    url_abort("parameter", "Parameters in `...` must be named, such as `token = \"...\"`.")
  }
  values <- vapply(params, function(v) {
    if (is.logical(v) && length(v) == 1L && !is.na(v)) {
      return(tolower(as.character(v)))
    }
    if (!(is.character(v) || is.numeric(v)) || length(v) != 1L || is.na(v)) {
      url_abort("parameter", "Each parameter in `...` must be a single string, number or logical.")
    }
    as.character(v)
  }, character(1))
  tail <- paste0(";", keys, "=", vapply(values, utils::URLencode, "", reserved = TRUE), collapse = "")
  # Parameters follow the path's ';'; a URL without a path gets "/".
  authority_end <- regexpr("/", substr(url, 6L, nchar(url)), fixed = TRUE)
  if (authority_end < 0L) {
    url <- paste0(url, "/")
  }
  paste0(url, tail)
}

# The headers sent on every call: the string's unreserved parameters, then
# `headers`, then the bearer token, as PySpark's access-token credentials
# send it.
connection_metadata <- function(u, headers = NULL) {
  metadata <- check_header_text(c(u$headers, check_headers(headers)))
  if (!is.null(u$token)) {
    metadata[["authorization"]] <- paste("Bearer", u$token)
  }
  metadata
}

# A header gRPC can send: the name an HTTP token, the value printable
# ASCII. zugrpc refuses the rest with its own error class; zuspark names
# the header instead.
check_header_text <- function(headers) {
  bad_name <- !grepl("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$", names(headers))
  bad_value <- !grepl("^[\\x20-\\x7E\t]*$", headers, perl = TRUE)
  bad <- bad_name | bad_value
  if (any(bad)) {
    url_abort(
      "headers",
      sprintf(
        "Header '%s' can't be sent: a name must be letters, digits or !#$%%&'*+.^_`|~-, and a value printable ASCII.",
        names(headers)[bad][[1L]]
      )
    )
  }
  headers
}

# NULL, or a single non-empty string.
check_text_arg <- function(x, name) {
  if (!is.null(x) && (!is.character(x) || length(x) != 1L || is.na(x) || !nzchar(x))) {
    url_abort(name, sprintf("`%s` must be NULL or a single non-empty string.", name))
  }
  invisible(x)
}

# NULL, a positive number of seconds, or a date-time to stop at.
check_deadline <- function(deadline) {
  ok <- is.null(deadline) ||
    (length(deadline) == 1L && inherits(deadline, "POSIXct") && !is.na(deadline)) ||
    (is.numeric(deadline) && length(deadline) == 1L && !is.na(deadline) && deadline > 0)
  if (!ok) {
    url_abort("deadline", "`deadline` must be NULL, a positive number of seconds, or a date-time.")
  }
  invisible(deadline)
}

check_headers <- function(headers) {
  if (is.null(headers)) {
    return(character())
  }
  if (is.list(headers)) {
    headers <- unlist(headers)
  }
  if (!is.character(headers) || is.null(names(headers)) || any(!nzchar(names(headers)))) {
    zuspark_abort("zuspark_error", "`headers` must be a named character vector.")
  }
  headers
}
