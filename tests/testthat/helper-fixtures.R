# The fake channel of design §9: a transport that replays a scenario
# recorded by tools/record-fixtures.R, call by call, with no network.
#
# local_replay() installs it for the calling test and returns an
# environment whose `requests` collects what zuspark sent, decoded, and
# `methods` the methods called, so tests can assert on the request shape.
# `responses` replaces recorded responses by call number. For a unary call,
# a raw vector is returned as the response, and a function is called with
# the recorded bytes and may signal a condition instead. For a streaming
# call, a function is called with the list of recorded messages and returns
# the list to send; an element that is a condition is signalled when the
# stream reaches it.

fixture_dir <- function(scenario) {
  test_path("fixtures", scenario)
}

fixture_calls <- function(scenario) {
  requests <- sort(list.files(fixture_dir(scenario), "\\.request\\.binpb$"))
  stem <- sub("\\.request\\.binpb$", "", requests)
  data.frame(
    stem = stem,
    method = sub("^[0-9]+-", "", stem),
    stringsAsFactors = FALSE
  )
}

read_binpb <- function(path) {
  readBin(path, "raw", file.size(path))
}

read_fixture <- function(scenario, stem, part) {
  read_binpb(file.path(fixture_dir(scenario), paste0(stem, ".", part, ".binpb")))
}

# The response messages of a streaming call, in order.
read_stream_fixture <- function(scenario, stem) {
  pattern <- paste0("^", stem, "\\.response\\.[0-9]+\\.binpb$")
  files <- sort(list.files(fixture_dir(scenario), pattern, full.names = TRUE))
  lapply(files, read_binpb)
}

streaming_methods <- c("ExecutePlan", "ReattachExecute")

request_types <- c(
  Config = "spark.connect.ConfigRequest",
  AnalyzePlan = "spark.connect.AnalyzePlanRequest",
  ReleaseSession = "spark.connect.ReleaseSessionRequest",
  ExecutePlan = "spark.connect.ExecutePlanRequest",
  ReattachExecute = "spark.connect.ReattachExecuteRequest",
  ReleaseExecute = "spark.connect.ReleaseExecuteRequest"
)
response_types <- c(
  Config = "spark.connect.ConfigResponse",
  AnalyzePlan = "spark.connect.AnalyzePlanResponse",
  ReleaseSession = "spark.connect.ReleaseSessionResponse",
  ExecutePlan = "spark.connect.ExecutePlanResponse",
  ReattachExecute = "spark.connect.ExecutePlanResponse",
  ReleaseExecute = "spark.connect.ReleaseExecuteResponse"
)

decode_fixture <- function(scenario, n, part = "response") {
  call <- fixture_calls(scenario)[n, ]
  types <- if (part == "request") request_types else response_types
  zuproto::proto_decode(
    read_fixture(scenario, call$stem, part),
    types[[call$method]],
    spark_pool()
  )
}

# The decoded response messages of streaming call `n`.
decode_stream_fixture <- function(scenario, n) {
  call <- fixture_calls(scenario)[n, ]
  lapply(read_stream_fixture(scenario, call$stem), function(m) {
    zuproto::proto_decode(m, response_types[[call$method]], spark_pool())
  })
}

encode_response <- function(x, method) {
  zuproto::proto_encode(x, response_types[[method]], spark_pool())
}

# A fake stream over a list of messages, any of which may be a condition.
replay_stream <- function(messages) {
  i <- 0L
  list(
    next_message = function() {
      i <<- i + 1L
      if (i > length(messages)) {
        return(NULL)
      }
      m <- messages[[i]]
      if (inherits(m, "condition")) stop(m)
      m
    },
    cancel = function() invisible()
  )
}

local_replay <- function(scenario, responses = list(), env = parent.frame()) {
  calls <- fixture_calls(scenario)
  state <- new.env(parent = emptyenv())
  state$n <- 0L
  state$requests <- list()
  state$methods <- character()

  transport <- function(con, method, bytes, deadline, streaming = FALSE) {
    n <- state$n + 1L
    state$n <- n
    if (n > nrow(calls)) {
      stop(sprintf("Fixture '%s' has %d calls; call %d was %s.", scenario, nrow(calls), n, method))
    }
    if (calls$method[[n]] != method) {
      stop(sprintf("Fixture '%s' call %d is %s, not %s.", scenario, n, calls$method[[n]], method))
    }
    state$methods <- c(state$methods, method)
    state$requests[[n]] <- zuproto::proto_decode(bytes, request_types[[method]], spark_pool())
    override <- responses[[as.character(n)]]
    if (streaming) {
      messages <- read_stream_fixture(scenario, calls$stem[[n]])
      if (is.function(override)) messages <- override(messages)
      return(replay_stream(messages))
    }
    recorded <- read_fixture(scenario, calls$stem[[n]], "response")
    if (is.function(override)) {
      return(override(recorded))
    }
    if (!is.null(override)) {
      return(override)
    }
    recorded
  }
  withr::local_options(zuspark.transport = transport, .local_envir = env)

  # Executions get the operation ids they were recorded with, in order, so
  # that the recorded responses answer for the operation zuspark runs.
  ids <- vapply(which(calls$method == "ExecutePlan"), function(n) {
    decode_fixture(scenario, n, "request")$operation_id
  }, character(1))
  state$operation <- 0L
  local_mocked_bindings(
    new_operation_id = function() {
      state$operation <- state$operation + 1L
      if (state$operation <= length(ids)) ids[[state$operation]] else zuspark_uuid()
    },
    .package = "zuspark",
    .env = env
  )
  state
}

zuspark_uuid <- function() as.character(zuid::uuid(version = 4))

# The session id a scenario was recorded with, from its first request.
fixture_session_id <- function(scenario) {
  decode_fixture(scenario, 1, "request")$session_id
}

# Connects through the replay installed by local_replay(scenario).
replay_connect <- function(scenario, url = "sc://fixture.invalid", ...) {
  spark_connect(url, session_id = fixture_session_id(scenario), user_id = "zuspark", ...)
}

# A condition shaped as zugrpc raises it for a non-OK status, or for a
# transport failure.
grpc_status_condition <- function(status, message = status) {
  structure(
    class = c(
      paste0("zugrpc_error_", tolower(status)),
      "zugrpc_error_status", "zugrpc_error", "error", "condition"
    ),
    list(message = paste0(status, ": ", message), call = NULL, grpc_status = status)
  )
}

grpc_transport_condition <- function(message = "connection reset") {
  structure(
    class = c(
      "zugrpc_error_transport", "zugrpc_error_unavailable", "zugrpc_error",
      "error", "condition"
    ),
    list(message = message, call = NULL)
  )
}

# Rewrites one decoded ExecutePlanResponse inside a recorded stream.
edit_response <- function(messages, i, edit) {
  x <- zuproto::proto_decode(messages[[i]], "spark.connect.ExecutePlanResponse", spark_pool())
  messages[[i]] <- encode_response(edit(x), "ExecutePlan")
  messages
}

# The index of the first message of a stream with member `member`.
which_response <- function(messages, member) {
  for (i in seq_along(messages)) {
    x <- zuproto::proto_decode(messages[[i]], "spark.connect.ExecutePlanResponse", spark_pool())
    if (!is.null(x[[member]])) return(i)
  }
  NA_integer_
}
