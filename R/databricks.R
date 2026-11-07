# Databricks (design §7.8). A workspace speaks Spark Connect on port 443
# over TLS, with a bearer token, and needs one header to pick the compute:
# x-databricks-session-id for serverless, x-databricks-cluster-id for a
# cluster. Verified against a Free Edition workspace on 2026-11-06; see
# .agents/databricks.md.

#' Connect to Databricks
#'
#' `spark_connect_databricks()` opens a Spark Connect session on a
#' Databricks workspace: on serverless compute by default, or on a cluster
#' given by its id. It is [spark_connect()] with the connection string and
#' the header Databricks needs to choose the compute: for serverless, an
#' `x-databricks-session-id` header carrying the session id; for a
#' cluster, an `x-databricks-cluster-id` header. The cluster must be
#' running.
#'
#' @param host The workspace URL, such as
#'   `"https://dbc-1a2b3c4d-5e6f.cloud.databricks.com"`, with or without
#'   `https://`. It defaults to the `DATABRICKS_HOST` environment variable.
#' @param token A personal access token. It defaults to the
#'   `DATABRICKS_TOKEN` environment variable.
#' @param cluster_id `NULL` for serverless compute, or the id of a cluster.
#' @param ... Passed on to [spark_connect()]: `user_id`, `user_agent`,
#'   `deadline`, or more connection string parameters.
#' @param session_id The session's id, a UUID; by default a new version 4
#'   UUID. On serverless compute it is also sent as
#'   `x-databricks-session-id`.
#' @inheritParams spark_connect
#' @return A `zuspark_connection`.
#' @seealso The [Connect to
#'   Databricks](https://pedrobtz.github.io/zuspark/articles/databricks.html)
#'   article.
#' @export
#' @examplesIf nzchar(Sys.getenv("DATABRICKS_TOKEN"))
#' con <- spark_connect_databricks()
#' spark_version(con)
#' spark_disconnect(con)
spark_connect_databricks <- function(host = Sys.getenv("DATABRICKS_HOST"),
                                     token = Sys.getenv("DATABRICKS_TOKEN"),
                                     cluster_id = NULL, ...,
                                     session_id = NULL, headers = NULL) {
  host <- databricks_host(host)
  if (!is_string(token)) {
    url_abort("token", "No Databricks token: pass `token`, or set DATABRICKS_TOKEN.")
  }
  headers <- check_headers(headers)
  if (is.null(cluster_id)) {
    session_id <- session_id %||% new_uuid()
    check_session_id(session_id)
    session_id <- as.character(unclass(session_id))
    headers[["x-databricks-session-id"]] <- session_id
  } else {
    if (!is_string(cluster_id)) {
      url_abort("cluster_id", "`cluster_id` must be NULL or a cluster's id.")
    }
    headers[["x-databricks-cluster-id"]] <- cluster_id
  }
  spark_connect(
    sprintf("sc://%s:443/;use_ssl=true", host),
    token = token, ...,
    session_id = session_id, headers = headers
  )
}

# The workspace's host name, from a URL as the browser shows it or a bare
# host name.
databricks_host <- function(host) {
  if (!is_string(host)) {
    url_abort("host", "No Databricks workspace: pass `host`, or set DATABRICKS_HOST.")
  }
  host <- sub("^[A-Za-z][A-Za-z0-9+.-]*://", "", trimws(host))
  host <- sub("[/?#].*$", "", host)
  host <- sub(":443$", "", host)
  if (!nzchar(host) || grepl("[:;@ ]", host)) {
    url_abort("host", sprintf("'%s' is not a Databricks workspace's host name.", host))
  }
  host
}
