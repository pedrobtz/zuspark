# Every RPC goes through spark_rpc(): encode with zuproto, send through the
# transport, decode the answer. The transport is a function
# (con, method, bytes, deadline) returning the response message's bytes,
# normally a unary zugrpc call on the connection's channel. Tests replace it
# through the internal option `zuspark.transport` with a fake channel that
# replays recorded bytes (design §9); tools/record-fixtures.R records them
# by setting `zuspark.record` to a directory.

spark_service <- "/spark.connect.SparkConnectService/"

spark_rpc <- function(con, method, request, request_type, response_type,
                      deadline = con$deadline) {
  pool <- spark_pool()
  bytes <- zuproto::proto_encode(request, request_type, pool)
  # Bound when the connection was made, so a finalizer that runs later
  # uses the same transport.
  response <- con$transport(con, method, bytes, deadline)
  record_rpc(method, bytes, response)
  zuproto::proto_decode(response, response_type, pool, enums = "character")
}

grpc_transport <- function(con, method, bytes, deadline) {
  res <- zugrpc::grpc_call(
    con$channel,
    paste0(spark_service, method),
    bytes,
    deadline = deadline
  )
  res$message
}

# With `zuspark.record` set to a directory, each call's request and
# response bytes are written there as <nn>-<method>.request.binpb and
# .response.binpb, numbered in call order.
record_rpc <- function(method, request, response) {
  dir <- getOption("zuspark.record")
  if (is.null(dir)) {
    return(invisible())
  }
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  n <- length(list.files(dir, pattern = "\\.request\\.binpb$")) + 1L
  stem <- file.path(dir, sprintf("%02d-%s", n, method))
  writeBin(request, paste0(stem, ".request.binpb"))
  writeBin(response, paste0(stem, ".response.binpb"))
  invisible()
}
