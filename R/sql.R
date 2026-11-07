# SQL (design §7.3): the two-step protocol. spark_sql() sends the statement
# as a SqlCommand. A command runs at once and its rows come back in the
# answer; a query comes back as its relation, unexecuted, and each
# spark_collect() or spark_fetch() executes it afresh.

#' Run SQL on the server
#'
#' `spark_sql()` sends a statement to the server. A command (DDL, DML,
#' `SET`, `SHOW`, a SQL script) runs at once, and its rows come back as a
#' data frame. A query (`SELECT`, `WITH`, `VALUES`, ...) comes back as a
#' `zuspark_query`, which holds the server's plan for it and no rows; it
#' runs when `spark_collect()` or `spark_fetch()` asks for them, and runs
#' again on every such call. `spark_execute()` runs a statement for its
#' effect and discards its rows.
#'
#' @inheritParams spark_disconnect
#' @param sql A Spark SQL statement, as a single string.
#'
#' @return `spark_sql()` returns a `zuspark_query` for a query, or a data
#'   frame for a command. `spark_execute()` returns the number of rows the
#'   statement produced, invisibly.
#' @export
#' @examplesIf nzchar(Sys.getenv("SPARK_REMOTE"))
#' con <- spark_connect()
#' q <- spark_sql(con, "SELECT id, id * 2 AS twice FROM range(5)")
#' q
#' spark_collect(q)
#' spark_sql(con, "SHOW DATABASES")
#' spark_disconnect(con)
spark_sql <- function(con, sql) {
  check_open(con)
  check_sql(sql)
  relation <- NULL
  keep_relation <- function(response) {
    if (!is.null(response$sql_command_result)) {
      relation <<- response$sql_command_result$relation
    }
  }
  execute_plan(con, sql_command_plan(sql), on_response = keep_relation)
  if (is.null(relation)) {
    zuspark_abort(
      "zuspark_protocol_error",
      "The server answered a SQL command without a SqlCommandResult."
    )
  }
  # A command's rows, already computed, as one Arrow IPC stream. Anything
  # else is a plan to execute: the query's own relation, or a cached result.
  if (!is.null(relation$local_relation)) {
    return(read_command_rows(relation$local_relation$data))
  }
  new_query(con, sql, relation)
}

#' @rdname spark_sql
#' @export
spark_execute <- function(con, sql) {
  x <- spark_sql(con, sql)
  if (is.data.frame(x)) {
    return(invisible(nrow(x)))
  }
  rows <- 0
  count_rows <- function(response) {
    if (!is.null(response$arrow_batch)) {
      rows <<- rows + (response$arrow_batch$row_count %||% 0)
    }
  }
  execute_plan(con, list(root = x$relation), on_response = count_rows)
  invisible(rows)
}

#' Fetch a query's result
#'
#' `spark_collect()` runs a query and returns its result as one data frame.
#' `spark_fetch()` runs it and returns the result as an Arrow stream, for
#' code that consumes Arrow: 'zuarrow', 'nanoarrow', or `arrow_to_r()` with
#' its own conversion options. Each call runs the query again on the
#' server. `as.data.frame()` on a query is `spark_collect()`.
#'
#' Types reach R as 'zuarrow' converts them. A column R cannot hold
#' exactly, such as a `BIGINT` beyond 2^53 without 'zuint' loaded, is an
#' error by default; `lossy` chooses text or a widened type instead.
#'
#' @param x A `zuspark_query` from [spark_sql()].
#' @param lossy,to Passed to [zuarrow::arrow_to_r()]: what to do with a
#'   column R cannot hold exactly, and prototypes for the columns.
#' @param row.names,optional,... For `as.data.frame()`: not used.
#'
#' @return `spark_collect()` returns a data frame. `spark_fetch()` returns
#'   a 'zuarrow' stream handle, which is a 'nanoarrow' `ArrowArrayStream`.
#' @export
#' @examplesIf nzchar(Sys.getenv("SPARK_REMOTE"))
#' con <- spark_connect()
#' q <- spark_sql(con, "SELECT id FROM range(3)")
#' spark_collect(q)
#' stream <- spark_fetch(q)
#' zuarrow::arrow_to_r(stream)
#' spark_disconnect(con)
spark_collect <- function(x, lossy = "error", to = NULL) {
  check_query(x)
  fetched <- fetch_batches(x)
  df <- arrow_call(zuarrow::arrow_to_r(read_batches(fetched$batches), to = to, lossy = lossy))
  if (nrow(df) != fetched$rows) {
    zuspark_abort(
      "zuspark_protocol_error",
      sprintf(
        "The server counted %s rows, but its batches hold %s.",
        format(fetched$rows), format(nrow(df))
      )
    )
  }
  df
}

#' @rdname spark_collect
#' @export
spark_fetch <- function(x) {
  check_query(x)
  read_batches(fetch_batches(x)$batches)
}

#' @rdname spark_collect
#' @export
as.data.frame.zuspark_query <- function(x, row.names = NULL, optional = FALSE, ...,
                                        lossy = "error", to = NULL) {
  spark_collect(x, lossy = lossy, to = to)
}

#' @export
print.zuspark_query <- function(x, ...) {
  sql <- gsub("\\s+", " ", x$sql)
  if (nchar(sql) > 70L) sql <- paste0(substr(sql, 1L, 67L), "...")
  cat(sprintf("<zuspark_query> %s\n", sql))
  runs <- if (is.null(x$rows)) "not run yet" else sprintf("last run: %s rows", format(x$rows))
  cat(sprintf("  on %s (%s)\n", x$con$url, runs))
  invisible(x)
}

# Both fields of the command (design Q2, decided 2026-11-05): `input` for
# Spark 4, which prefers it, and the deprecated `sql` for Spark 3.5, which
# has no `input` and ignores the unknown field.
sql_command_plan <- function(sql) {
  list(command = list(sql_command = list(
    sql = sql,
    input = list(sql = list(query = sql))
  )))
}

# A query is an environment, so that the metrics of its last execution are
# kept on it.
new_query <- function(con, sql, relation) {
  x <- new.env(parent = emptyenv())
  x$con <- con
  x$sql <- sql
  x$relation <- relation
  x$rows <- NULL
  x$metrics <- NULL
  x$observed_metrics <- list()
  x$skipped <- 0L
  x$reattaches <- 0L
  x$operation_id <- NULL
  class(x) <- "zuspark_query"
  x
}

# Executes the query's relation and collects its ArrowBatch payloads, each
# a whole IPC stream (design §5). start_offset, when the server sends it,
# must be the running total of the rows before it.
fetch_batches <- function(x) {
  check_open(x$con)
  batches <- vector("list", 16L)
  n <- 0L
  rows <- 0
  keep_operation <- function(op) {
    x$operation_id <- op
  }
  keep_batch <- function(response) {
    batch <- response$arrow_batch
    if (is.null(batch)) {
      return()
    }
    offset <- batch$start_offset
    if (!is.null(offset) && offset != rows) {
      zuspark_abort(
        "zuspark_protocol_error",
        sprintf("A batch starts at row %s, but %s rows came before it.", format(offset), format(rows))
      )
    }
    rows <<- rows + (batch$row_count %||% 0)
    n <<- n + 1L
    if (n > length(batches)) length(batches) <<- 2L * length(batches)
    batches[[n]] <<- batch$data %||% raw()
  }
  run <- execute_plan(
    x$con, list(root = x$relation),
    on_response = keep_batch, on_operation = keep_operation
  )
  x$rows <- rows
  x$metrics <- run$metrics
  x$observed_metrics <- run$observed_metrics
  x$skipped <- run$skipped
  x$reattaches <- run$reattaches
  if (n == 0L) {
    zuspark_abort(
      "zuspark_protocol_error",
      "The server finished the query without sending a batch, not even its schema."
    )
  }
  list(batches = batches[seq_len(n)], rows = rows)
}

read_batches <- function(batches) {
  arrow_call(zuarrow::arrow_read(batches, lazy = TRUE))
}

read_command_rows <- function(data) {
  if (is.null(data) || !length(data)) {
    return(data.frame())
  }
  arrow_call(zuarrow::arrow_read(data))
}

# zuarrow's format, limit and codec errors are how it reports hostile
# input: the server sent bytes that are not what the protocol says (design
# §7.5), or that claim more than zuarrow's limits allow, or a compressed
# buffer that does not decompress. Everything else, a lossy column for
# one, reaches the caller as zuarrow raised it.
arrow_call <- function(expr) {
  tryCatch(
    expr,
    zuarrow_format_error = function(e) arrow_protocol_error(e, "is malformed"),
    zuarrow_limit_error = function(e) arrow_protocol_error(e, "is over zuarrow's limits"),
    zuarrow_codec_error = function(e) arrow_protocol_error(e, "does not decompress")
  )
}

arrow_protocol_error <- function(e, what) {
  zuspark_abort(
    "zuspark_protocol_error",
    sprintf("The server's Arrow data %s: %s", what, conditionMessage(e)),
    parent = e
  )
}

check_sql <- function(sql) {
  if (!is_string(sql)) {
    zuspark_abort("zuspark_error", "`sql` must be a single, non-empty string.")
  }
  invisible(sql)
}

check_query <- function(x) {
  if (!inherits(x, "zuspark_query")) {
    zuspark_abort(
      "zuspark_error",
      "`x` must be a query from `spark_sql()`; a command's rows are already a data frame."
    )
  }
  invisible(x)
}
