# The fake channel of design §9: a transport that replays a scenario
# recorded by tools/record-fixtures.R, call by call, with no network.
#
# local_replay() installs it for the calling test and returns an
# environment whose `requests` collects what zuspark sent, decoded, so tests
# can assert on the request shape. `responses` replaces recorded responses by
# call number: a raw vector is returned as the response; a function is
# called with the recorded bytes, and may signal a condition instead.

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

read_fixture <- function(scenario, stem, part) {
  path <- file.path(fixture_dir(scenario), paste0(stem, ".", part, ".binpb"))
  readBin(path, "raw", file.size(path))
}

request_types <- c(
  Config = "spark.connect.ConfigRequest",
  AnalyzePlan = "spark.connect.AnalyzePlanRequest",
  ReleaseSession = "spark.connect.ReleaseSessionRequest"
)
response_types <- sub("Request$", "Response", request_types)

decode_fixture <- function(scenario, n, part = "response") {
  call <- fixture_calls(scenario)[n, ]
  types <- if (part == "request") request_types else response_types
  zuproto::proto_decode(
    read_fixture(scenario, call$stem, part),
    types[[call$method]],
    spark_pool()
  )
}

encode_response <- function(x, method) {
  zuproto::proto_encode(x, response_types[[method]], spark_pool())
}

local_replay <- function(scenario, responses = list(), env = parent.frame()) {
  calls <- fixture_calls(scenario)
  state <- new.env(parent = emptyenv())
  state$n <- 0L
  state$requests <- list()
  state$methods <- character()

  transport <- function(con, method, bytes, deadline) {
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
    recorded <- read_fixture(scenario, calls$stem[[n]], "response")
    override <- responses[[as.character(n)]]
    if (is.function(override)) {
      return(override(recorded))
    }
    if (!is.null(override)) {
      return(override)
    }
    recorded
  }
  withr::local_options(zuspark.transport = transport, .local_envir = env)
  state
}

# The session id a scenario was recorded with, from its first request.
fixture_session_id <- function(scenario) {
  decode_fixture(scenario, 1, "request")$session_id
}

# Connects through the replay installed by local_replay(scenario).
replay_connect <- function(scenario, url = "sc://fixture.invalid", ...) {
  spark_connect(url, session_id = fixture_session_id(scenario), user_id = "zuspark", ...)
}

# A condition shaped as zugrpc raises it for a non-OK status.
grpc_status_condition <- function(status, message = status) {
  structure(
    class = c(
      paste0("zugrpc_error_", tolower(status)),
      "zugrpc_error_status", "zugrpc_error", "error", "condition"
    ),
    list(message = paste0(status, ": ", message), call = NULL, grpc_status = status)
  )
}
