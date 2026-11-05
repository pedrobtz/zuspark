# ExecutePlan with reattachable execution (design §7.4), after PySpark's
# reattach.py. Every execution carries a client-made operation id and asks
# to be reattachable; the server may then end a stream without
# ResultComplete, and the client reattaches from the last response it
# consumed. Responses already consumed are released every
# `release_every` responses, inline on the same channel (D13), and all of
# them on completion or when the execution is abandoned. Nothing else is
# retried (D8).

release_every <- 16L

# The responses zuspark reads; any other member of the response_type oneof
# is skipped and counted, so that a newer server's messages do not break a
# result.
known_responses <- c(
  "arrow_batch", "sql_command_result", "result_complete", "schema",
  "metrics", "observed_metrics", "execution_progress"
)
response_header <- c("session_id", "server_side_session_id", "operation_id", "response_id")

# Runs `plan` and calls `on_response(response)` for each response with a
# member other than the header. Returns a list of `operation_id`,
# `responses` (how many), `reattaches`, `skipped` (responses of unknown
# types), `schema`, `metrics` and `observed_metrics`.
execute_plan <- function(con, plan, on_response = function(response) NULL) {
  check_open(con)
  op <- new_operation_id()
  run <- new.env(parent = emptyenv())
  run$last_response_id <- NULL
  run$unreleased <- 0L
  run$responses <- 0L
  run$reattaches <- 0L
  run$skipped <- 0L
  run$complete <- FALSE
  run$schema <- NULL
  run$metrics <- NULL
  run$observed_metrics <- list()

  # Abandoned (an error, an interrupt): release everything the server
  # still buffers for this operation, best effort.
  on.exit(
    if (!run$complete) try(release_execute(con, op, all = TRUE), silent = TRUE),
    add = TRUE
  )

  request <- c(
    session_fields(con),
    list(
      operation_id = op,
      plan = plan,
      request_options = list(list(reattach_options = list(reattachable = TRUE)))
    )
  )
  stream <- open_execute(con, request)
  stalled <- 0L
  repeat {
    before <- run$responses
    ended <- read_execute_stream(con, op, stream, run, on_response)
    if (run$complete) {
      break
    }
    # The stream ended, or failed, without ResultComplete: reattach.
    stalled <- if (run$responses > before) 0L else stalled + 1L
    if (stalled > 2L) {
      zuspark_abort(
        "zuspark_protocol_error",
        sprintf("Operation %s made no progress across three reattaches.", op),
        operation_id = op
      )
    }
    stream <- reattach_execute(con, op, run, request, ended)
    run$reattaches <- run$reattaches + 1L
  }
  release_execute(con, op, all = TRUE)
  list(
    operation_id = op,
    responses = run$responses,
    reattaches = run$reattaches,
    skipped = run$skipped,
    schema = run$schema,
    metrics = run$metrics,
    observed_metrics = run$observed_metrics
  )
}

# A fresh id per execution; the tests replay recorded ids through it.
new_operation_id <- function() {
  new_uuid()
}

open_execute <- function(con, request) {
  spark_stream(
    con, "ExecutePlan", request,
    "spark.connect.ExecutePlanRequest", "spark.connect.ExecutePlanResponse"
  )
}

# Reads `stream` to its end. Returns NULL when it ended cleanly, or the
# transport condition that cut it; a server status other than OK is raised.
read_execute_stream <- function(con, op, stream, run, on_response) {
  repeat {
    response <- tryCatch(
      stream$next_response(),
      zugrpc_error_transport = function(e) e
    )
    if (inherits(response, "condition")) {
      return(response)
    }
    if (is.null(response)) {
      return(NULL)
    }
    observe_session(con, response)
    if (nzchar(response$operation_id %||% "") && !identical(response$operation_id, op)) {
      zuspark_abort(
        "zuspark_protocol_error",
        sprintf("A response for operation %s arrived on operation %s.", response$operation_id, op),
        operation_id = op
      )
    }
    run$responses <- run$responses + 1L
    run$last_response_id <- response$response_id %||% run$last_response_id

    members <- setdiff(names(response), response_header)
    if (!is.null(response$schema)) run$schema <- response$schema
    if (!is.null(response$metrics)) run$metrics <- response$metrics
    if (length(response$observed_metrics)) {
      run$observed_metrics <- c(run$observed_metrics, response$observed_metrics)
    }
    unknown <- setdiff(members, known_responses)
    if (length(unknown)) {
      run$skipped <- run$skipped + 1L
    }
    if (length(intersect(members, c("arrow_batch", "sql_command_result")))) {
      on_response(response)
    }
    if (!is.null(response$result_complete)) {
      run$complete <- TRUE
    }

    run$unreleased <- run$unreleased + 1L
    if (!run$complete && run$unreleased >= release_every && !is.null(run$last_response_id)) {
      release_execute(con, op, until = run$last_response_id)
      run$unreleased <- 0L
    }
  }
}

# Reattach from the last consumed response. A stream that failed before any
# response may never have reached the server: reattach then answers
# INVALID_HANDLE.OPERATION_NOT_FOUND, and only then is the ExecutePlan
# re-sent, since the statement cannot have run.
reattach_execute <- function(con, op, run, request, cut) {
  fields <- c(session_fields(con), list(operation_id = op))
  if (!is.null(run$last_response_id)) {
    fields$last_response_id <- run$last_response_id
  }
  stream <- spark_stream(
    con, "ReattachExecute", fields,
    "spark.connect.ReattachExecuteRequest", "spark.connect.ExecutePlanResponse"
  )
  if (run$responses > 0L) {
    return(stream)
  }
  # Nothing consumed yet: look at the first answer before committing to it.
  first <- tryCatch(
    stream$next_response(),
    zugrpc_error_status = function(e) e
  )
  if (inherits(first, "condition")) {
    if (grepl("OPERATION_NOT_FOUND", conditionMessage(first), fixed = TRUE)) {
      return(open_execute(con, request))
    }
    stop(first)
  }
  pushed_back(first, stream)
}

# A stream whose first response has already been read.
pushed_back <- function(first, stream) {
  pending <- list(first)
  list(
    next_response = function() {
      if (length(pending)) {
        r <- pending[[1L]]
        pending <<- list()
        return(r)
      }
      stream$next_response()
    },
    cancel = stream$cancel
  )
}

release_execute <- function(con, op, all = FALSE, until = NULL) {
  request <- c(session_fields(con), list(operation_id = op))
  if (all) {
    request$release_all <- list()
  } else {
    request$release_until <- list(response_id = until)
  }
  response <- spark_rpc(
    con, "ReleaseExecute", request,
    "spark.connect.ReleaseExecuteRequest", "spark.connect.ReleaseExecuteResponse"
  )
  observe_session(con, response)
  invisible()
}
