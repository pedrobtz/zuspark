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
response_header_fields <- c("session_id", "server_side_session_id", "operation_id", "response_id")

# Runs `plan` and calls `on_response(response)` for each response that
# carries an `arrow_batch` or a `sql_command_result`; the schema, metrics
# and progress responses are kept on the returned list instead. Returns a list of `operation_id`,
# `responses` (how many), `reattaches`, `skipped` (responses of unknown
# types), `schema`, `metrics` and `observed_metrics`.
execute_plan <- function(con, plan, on_response = function(response) NULL,
                         on_operation = function(op) NULL) {
  check_open(con)
  op <- new_operation_id()
  on_operation(op)
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
  # The stream still open, if any, is cancelled first, so that the
  # channel does not keep an abandoned HTTP/2 stream (#32).
  run$stream <- NULL
  on.exit(
    if (!run$complete) {
      if (!is.null(run$stream)) try(run$stream$cancel(), silent = TRUE)
      try(release_execute(con, op, all = TRUE), silent = TRUE)
    },
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
  run$stream <- stream
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
    stream <- reattach_execute(con, op, run, request)
    run$stream <- stream
    run$reattaches <- run$reattaches + 1L
  }
  # The result is complete: the statement ran, whatever the release does.
  # A failed release must not read as a failed statement, which a caller
  # might run again (D8); the server frees the buffer when the session
  # ends. Not a retry: the release is sent once.
  release_best_effort(con, op, all = TRUE)
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
      zugrpc_error_transport = function(e) e,
      # Ctrl-C while waiting: the call is cancelled and the server told to
      # stop the job (design §7.9). zugrpc may deliver it as its own
      # condition or leave R's interrupt as it is.
      zugrpc_error_interrupt = function(e) interrupt_execution(con, op, stream),
      interrupt = function(e) interrupt_execution(con, op, stream)
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
    # An empty id, like a missing one, names nothing to resume from.
    if (nzchar(response$response_id %||% "")) {
      run$last_response_id <- response$response_id
    }

    members <- setdiff(names(response), response_header_fields)
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
      # Best effort, as the other releases: a read-only query whose release
      # fails still has its rows; the next release, or the last, frees what
      # this one did not (#32).
      release_best_effort(con, op, until = run$last_response_id)
      run$unreleased <- 0L
    }
  }
}

# Reattach from the last consumed response. A stream that failed before any
# response may never have reached the server: reattach then answers
# INVALID_HANDLE.OPERATION_NOT_FOUND, and only then is the ExecutePlan
# re-sent, since the statement cannot have run.
reattach_execute <- function(con, op, run, request) {
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
    zugrpc_error_transport = function(e) e,
    zugrpc_error_status = function(e) e
  )
  if (inherits(first, "zugrpc_error_transport")) {
    # A cut like any other: the stream gives the condition back to the
    # reader, which counts it as a stall and reattaches (#34).
    return(pushed_back(first, stream))
  }
  if (inherits(first, "condition")) {
    if (operation_not_found(first)) {
      return(open_execute(con, request))
    }
    stop(first)
  }
  pushed_back(first, stream)
}

# A stream whose first response has already been read.
# Whether a failed reattach says the server never received the operation.
# Spark's error class decides; the message is read only when the server
# sent no ErrorInfo (#34).
operation_not_found <- function(cnd) {
  class <- cnd$error_class %||% NA_character_
  if (!is.na(class)) {
    return(identical(class, "INVALID_HANDLE.OPERATION_NOT_FOUND"))
  }
  grepl("OPERATION_NOT_FOUND", conditionMessage(cnd), fixed = TRUE)
}

# The stream with `first` put back in front: a response, or a condition,
# which is raised again when read.
pushed_back <- function(first, stream) {
  pending <- list(first)
  list(
    next_response = function() {
      if (length(pending)) {
        r <- pending[[1L]]
        pending <<- list()
        if (inherits(r, "condition")) stop(r)
        return(r)
      }
      stream$next_response()
    },
    cancel = stream$cancel
  )
}

# A release whose call fails is dropped: the rows it covers are already
# read. A session error still propagates: the server lost the session.
release_best_effort <- function(con, op, ...) {
  tryCatch(release_execute(con, op, ...), zuspark_rpc_error = function(e) invisible())
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
