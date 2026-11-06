# The sc:// connection string (design §7.1), as PySpark's
# DefaultChannelBuilder reads it: the scheme is checked, the path must be
# empty, parameters follow the first ';' of the path as key=value pairs,
# percent-decoded and case-sensitive. Reserved parameters configure the
# channel and the session; every other parameter becomes a header sent on
# every call (D12).

spark_url_reserved <- c(
  "token", "use_ssl", "user_id", "user_agent", "session_id",
  "grpc_max_message_size",
  "grpc_keepalive_enabled", "grpc_keepalive_time_ms",
  "grpc_keepalive_timeout_ms", "grpc_keepalive_without_calls"
)

spark_url_example <- "Please update the URL to follow the correct format, e.g., 'sc://hostname:port'."

url_abort <- function(part, message) {
  zuspark_abort("zuspark_url_error", message, part = part)
}

# Returns a list: host, port, url (the http:// or https:// address for
# zugrpc), tls, token, user_id, user_agent, session_id, max_message,
# keepalive, headers and params (every parameter, as given).
parse_spark_url <- function(url) {
  if (!is.character(url) || length(url) != 1L || is.na(url)) {
    url_abort("url", "The connection string must be a single string.")
  }
  if (substr(url, 1L, 5L) != "sc://") {
    url_abort(
      "scheme",
      paste("The URL must start with 'sc://'.", spark_url_example)
    )
  }
  rest <- substr(url, 6L, nchar(url))
  # A query or fragment is not part of the grammar; like Python's parser,
  # drop it before splitting the authority from the path.
  rest <- sub("[?#].*$", "", rest)
  slash <- regexpr("/", rest, fixed = TRUE)
  if (slash > 0L) {
    authority <- substr(rest, 1L, slash - 1L)
    path <- substr(rest, slash, nchar(rest))
  } else {
    authority <- rest
    path <- ""
  }

  # Python's urlparse() takes parameters from after the first ';' of the
  # path's last segment; with an empty path that is the whole tail.
  semi <- regexpr(";", path, fixed = TRUE)
  params_text <- ""
  if (semi > 0L) {
    params_text <- substr(path, semi + 1L, nchar(path))
    path <- substr(path, 1L, semi - 1L)
  }
  if (nzchar(path) && path != "/") {
    url_abort(
      "path",
      sprintf("The path component '%s' must be empty. %s", path, spark_url_example)
    )
  }

  params <- parse_url_params(params_text)
  hostport <- parse_authority(authority, url)
  spark_url_settings(hostport$host, hostport$port, params)
}

parse_url_params <- function(text) {
  params <- character()
  if (!nzchar(text)) {
    return(params)
  }
  for (p in strsplit(text, ";", fixed = TRUE)[[1L]]) {
    kv <- strsplit(p, "=", fixed = TRUE)[[1L]]
    # strsplit() drops a trailing empty piece; "key=" is still a pair.
    if (endsWith(p, "=") && length(kv) == 1L) {
      kv <- c(kv, "")
    }
    if (length(kv) != 2L) {
      url_abort(
        "parameter",
        sprintf(
          paste(
            "Parameter '%s' should be provided as a key-value pair separated",
            "by an equal sign (=). Please update the parameter to follow the",
            "correct format, e.g., 'key=value'."
          ),
          # Only the key: a value, such as a token with '=' padding, may
          # be a credential.
          sub("=.*$", "", p)
        )
      )
    }
    params[[kv[[1L]]]] <- url_decode(kv[[1L]], kv[[2L]])
  }
  params
}

# Percent-decodes a parameter's value. utils::URLdecode() turns an invalid
# escape into a warning and a changed value, and %00 into an empty one; a
# token changed that way would still be sent, so both are refused.
url_decode <- function(key, value) {
  escapes <- regmatches(value, gregexpr("%.{0,2}", value))[[1L]]
  if (any(!grepl("^%[0-9A-Fa-f]{2}$", escapes)) || any(tolower(escapes) == "%00")) {
    url_abort(
      "parameter",
      sprintf("Parameter '%s' has an invalid percent-escape; write '%%' as '%%25'.", key)
    )
  }
  utils::URLdecode(value)
}

parse_authority <- function(authority, url) {
  # User information before '@' is not part of the grammar; Python drops it
  # from the host name, and so does zuspark.
  authority <- sub("^.*@", "", authority)
  port <- NULL
  if (startsWith(authority, "[")) {
    close <- regexpr("]", authority, fixed = TRUE)
    host <- if (close > 0L) substr(authority, 2L, close - 1L) else ""
    tail <- if (close > 0L) substr(authority, close + 1L, nchar(authority)) else ""
    if (startsWith(tail, ":")) {
      port <- substr(tail, 2L, nchar(tail))
    }
  } else {
    colon <- regexpr(":", authority, fixed = TRUE)
    if (colon > 0L) {
      host <- substr(authority, 1L, colon - 1L)
      port <- substr(authority, colon + 1L, nchar(authority))
    } else {
      host <- authority
    }
  }
  if (!nzchar(host)) {
    url_abort(
      "host",
      sprintf("Hostname is missing in the URL: '%s'. %s", redact_url(url), spark_url_example)
    )
  }
  host <- tolower(host)
  if (grepl(":", host, fixed = TRUE)) {
    host <- paste0("[", host, "]")
  }

  if (is.null(port) || !nzchar(port)) {
    port <- 15002L
  } else if (!grepl("^[0-9]+$", port) || as.numeric(port) > 65535) {
    url_abort(
      "port",
      sprintf("The port '%s' is not a number from 0 to 65535. %s", port, spark_url_example)
    )
  } else {
    port <- as.integer(port)
  }
  list(host = host, port = port)
}

spark_url_settings <- function(host, port, params) {
  get <- function(key) if (key %in% names(params)) params[[key]] else NULL

  token <- get("token")
  if (is.null(token)) {
    env <- Sys.getenv("SPARK_CONNECT_AUTHENTICATE_TOKEN")
    if (nzchar(env)) token <- env
  }
  if (!is.null(token) && !nzchar(token)) {
    url_abort("token", "Parameter 'token' is empty: give a token, or leave the parameter out.")
  }
  use_ssl <- identical(tolower(get("use_ssl") %||% ""), "true")
  # PySpark: a token makes the channel secure, but to localhost without
  # use_ssl it uses local credentials, which send the token in the clear.
  tls <- use_ssl || (!is.null(token) && host != "localhost")

  session_id <- get("session_id")
  if (!is.null(session_id)) {
    check_session_id(session_id)
  }

  max_message <- url_number(params, "grpc_max_message_size", 128 * 1024^2)
  keepalive <- zugrpc::grpc_keepalive(
    time = url_number(params, "grpc_keepalive_time_ms", 60000) / 1000,
    timeout = url_number(params, "grpc_keepalive_timeout_ms", 20000) / 1000
  )
  if (identical(tolower(get("grpc_keepalive_enabled") %||% ""), "false")) {
    keepalive <- NULL
  }

  list(
    host = host,
    port = port,
    url = sprintf("%s://%s:%d", if (tls) "https" else "http", host, port),
    tls = tls,
    token = token,
    user_id = get("user_id"),
    user_agent = get("user_agent"),
    session_id = session_id,
    max_message = max_message,
    keepalive = keepalive,
    headers = params[!names(params) %in% spark_url_reserved],
    params = params
  )
}

url_number <- function(params, key, default) {
  if (!key %in% names(params)) {
    return(default)
  }
  value <- suppressWarnings(as.numeric(params[[key]]))
  if (is.na(value) || !is.finite(value) || value <= 0) {
    url_abort(
      "parameter",
      sprintf("Parameter '%s' must be a positive number, not '%s'.", key, params[[key]])
    )
  }
  value
}

uuid_pattern <- "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"

check_session_id <- function(session_id) {
  if (!is.character(session_id) || length(session_id) != 1L ||
    is.na(session_id) || !grepl(uuid_pattern, session_id)) {
    url_abort(
      "session_id",
      sprintf("Parameter 'session_id' must be a UUID, not '%s'.", format(session_id))
    )
  }
  invisible(session_id)
}

# The connection string for display: the token's value never prints.
redact_url <- function(url) {
  gsub("(;token=)[^;]*", "\\1<redacted>", url)
}
