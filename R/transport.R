# Every RPC goes through spark_rpc() (unary) or spark_stream()
# (server-streaming): encode with zuproto, send through the transport,
# decode the answer. The transport is a function
# (con, method, bytes, deadline, streaming) returning, for a unary call, the
# response message's bytes and, for a streaming one, a list of `next`
# (a function returning the next message's bytes, or NULL at the end) and
# `cancel`. It is normally zugrpc on the connection's channel. Tests replace
# it through the internal option `zuspark.transport` with a fake channel
# that replays recorded bytes (design §9); tools/record-fixtures.R records
# them by setting `zuspark.record` to a directory.

spark_service <- "/spark.connect.SparkConnectService/"

spark_rpc <- function(con, method, request, request_type, response_type,
                      deadline = con$deadline) {
  pool <- spark_pool()
  bytes <- zuproto::proto_encode(request, request_type, pool)
  # Bound when the connection was made, so a finalizer that runs later
  # uses the same transport.
  response <- con$transport(con, method, bytes, deadline, streaming = FALSE)
  record_rpc(method, bytes, list(response))
  decode_response(response, response_type)
}

# A server-streaming call: returns a list of `next`, which returns the next
# decoded response or NULL at the end, and `cancel`.
spark_stream <- function(con, method, request, request_type, response_type,
                         deadline = NULL) {
  bytes <- zuproto::proto_encode(request, request_type, spark_pool())
  stream <- con$transport(con, method, bytes, deadline, streaming = TRUE)
  recorder <- stream_recorder(method, bytes)
  list(
    next_response = function() {
      message <- stream$next_message()
      recorder(message)
      if (is.null(message)) NULL else decode_response(message, response_type)
    },
    cancel = stream$cancel
  )
}

decode_response <- function(bytes, type) {
  tryCatch(
    zuproto::proto_decode(bytes, type, spark_pool(), enums = "character"),
    zuproto_error = function(e) {
      zuspark_abort(
        "zuspark_protocol_error",
        sprintf("The server sent a %s that does not decode.", sub("^spark\\.connect\\.", "", type)),
        parent = e
      )
    }
  )
}

grpc_transport <- function(con, method, bytes, deadline, streaming = FALSE) {
  path <- paste0(spark_service, method)
  if (!streaming) {
    return(zugrpc::grpc_call(con$channel, path, bytes, deadline = deadline)$message)
  }
  stream <- zugrpc::grpc_stream(con$channel, path, bytes, deadline = deadline)
  list(next_message = stream$next_message, cancel = stream$cancel)
}

# With `zuspark.record` set to a directory, each call's request is written
# there as <nn>-<method>.request.binpb and its response messages as
# <nn>-<method>.response.binpb (unary) or .response.<mmm>.binpb (streaming),
# numbered in call order.
record_stem <- function(method) {
  dir <- getOption("zuspark.record")
  if (is.null(dir)) {
    return(NULL)
  }
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  n <- length(list.files(dir, pattern = "\\.request\\.binpb$")) + 1L
  file.path(dir, sprintf("%02d-%s", n, method))
}

record_rpc <- function(method, request, responses) {
  stem <- record_stem(method)
  if (is.null(stem)) {
    return(invisible())
  }
  writeBin(request, paste0(stem, ".request.binpb"))
  writeBin(responses[[1L]], paste0(stem, ".response.binpb"))
  invisible()
}

stream_recorder <- function(method, request) {
  stem <- record_stem(method)
  if (is.null(stem)) {
    return(function(message) invisible())
  }
  writeBin(request, paste0(stem, ".request.binpb"))
  n <- 0L
  function(message) {
    if (!is.null(message)) {
      n <<- n + 1L
      writeBin(message, sprintf("%s.response.%03d.binpb", stem, n))
    }
    invisible()
  }
}
