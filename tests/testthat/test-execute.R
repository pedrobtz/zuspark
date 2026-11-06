# Reattachable execution and result checks (design §7.4, §7.5).

test_that("a result the server cut is reattached, every row once", {
  calls <- local_replay("reattach")
  con <- replay_connect("reattach")
  spark_config(con, spark.sql.execution.arrow.maxRecordsPerBatch = 100)
  q <- spark_sql(con, reattach_sql)
  df <- spark_collect(q)
  expect_identical(df$id, as.numeric(0:999))
  expect_gte(q$reattaches, 2L)
  spark_disconnect(con)
  expect_identical(sum(calls$methods == "ReattachExecute"), q$reattaches)
})

test_that("reattach resumes after the last consumed response", {
  calls <- local_replay("reattach")
  con <- replay_connect("reattach")
  spark_config(con, spark.sql.execution.arrow.maxRecordsPerBatch = 100)
  spark_collect(spark_sql(con, reattach_sql))
  spark_disconnect(con)
  methods <- calls$methods
  for (n in which(methods == "ReattachExecute")) {
    # The previous stream's last response, whatever call carried it.
    prev <- max(which(methods[seq_len(n - 1L)] %in% c("ExecutePlan", "ReattachExecute")))
    last <- decode_stream_fixture("reattach", prev)
    expect_identical(
      calls$requests[[n]]$last_response_id,
      last[[length(last)]]$response_id,
      label = sprintf("call %d", n)
    )
  }
})

test_that("consumed responses are released along the way and at the end", {
  calls <- local_replay("reattach")
  con <- replay_connect("reattach")
  spark_config(con, spark.sql.execution.arrow.maxRecordsPerBatch = 100)
  spark_collect(spark_sql(con, reattach_sql))
  spark_disconnect(con)
  releases <- calls$requests[calls$methods == "ReleaseExecute"]
  until <- Filter(function(r) !is.null(r$release_until), releases)
  expect_gte(length(until), 1L)
  expect_false(is.null(releases[[length(releases)]]$release_all))
})

test_that("a stream that fails before any response is re-sent only when the operation never arrived", {
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  recorded <- read_stream_fixture("sql-query", fixture_calls("sql-query")$stem[[5]])
  release <- read_fixture("sql-query", fixture_calls("sql-query")$stem[[6]], "response")
  sent <- character()
  con$transport <- function(con, method, bytes, deadline, streaming = FALSE) {
    sent <<- c(sent, method)
    switch(method,
      ExecutePlan = if (sum(sent == "ExecutePlan") == 1L) {
        replay_stream(list(grpc_transport_condition()))
      } else {
        replay_stream(recorded)
      },
      ReattachExecute = replay_stream(list(grpc_status_condition(
        "INTERNAL", "[INVALID_HANDLE.OPERATION_NOT_FOUND] The handle is invalid."
      ))),
      ReleaseExecute = release
    )
  }
  expect_identical(nrow(spark_collect(q)), 5L)
  expect_identical(sent, c("ExecutePlan", "ReattachExecute", "ExecutePlan", "ReleaseExecute"))
  close_connection(con)
})

test_that("other reattach failures are not retried", {
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  release <- read_fixture("sql-query", fixture_calls("sql-query")$stem[[6]], "response")
  con$transport <- function(con, method, bytes, deadline, streaming = FALSE) {
    switch(method,
      ExecutePlan = replay_stream(list(grpc_transport_condition())),
      ReattachExecute = replay_stream(list(grpc_status_condition("UNAVAILABLE", "gone"))),
      ReleaseExecute = release
    )
  }
  expect_error(spark_collect(q), class = "zugrpc_error_unavailable")
  close_connection(con)
})

test_that("a start_offset that is not the running total is a protocol error", {
  local_replay("sql-query", responses = list(`5` = function(messages) {
    i <- which_response(messages, "arrow_batch")
    edit_response(messages, i, function(x) {
      x$arrow_batch$start_offset <- 3
      x
    })
  }))
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_error(spark_collect(q), "starts at row 3", class = "zuspark_protocol_error")
})

test_that("a row_count that disagrees with the batches is a protocol error", {
  local_replay("sql-query", responses = list(`5` = function(messages) {
    # The last batch, so that no later start_offset notices first.
    i <- max(which(vapply(seq_along(messages), function(j) {
      !is.null(zuproto::proto_decode(messages[[j]], "spark.connect.ExecutePlanResponse", spark_pool())$arrow_batch)
    }, logical(1))))
    edit_response(messages, i, function(x) {
      x$arrow_batch$row_count <- x$arrow_batch$row_count + 1
      x
    })
  }))
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_error(spark_collect(q), "counted", class = "zuspark_protocol_error")
})

test_that("malformed Arrow data is a protocol error", {
  local_replay("sql-query", responses = list(`5` = function(messages) {
    i <- which_response(messages, "arrow_batch")
    edit_response(messages, i, function(x) {
      x$arrow_batch$data <- x$arrow_batch$data[seq_len(20)]
      x
    })
  }))
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_error(spark_collect(q), class = "zuspark_protocol_error")
})

test_that("a response that does not decode is a protocol error", {
  local_replay("sql-query", responses = list(`5` = function(messages) {
    messages[[1]] <- as.raw(c(0xff, 0xff, 0xff))
    messages
  }))
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_error(spark_collect(q), "does not decode", class = "zuspark_protocol_error")
})

test_that("a response of an unknown type is skipped and counted", {
  local_replay("sql-query", responses = list(`5` = function(messages) {
    i <- which_response(messages, "result_complete")
    extra <- zuproto::proto_decode(messages[[i]], "spark.connect.ExecutePlanResponse", spark_pool())
    extra$result_complete <- NULL
    extra$checkpoint_command_result <- list()
    append(messages, list(encode_response(extra, "ExecutePlan")), after = i - 1L)
  }))
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_identical(nrow(spark_collect(q)), 5L)
  expect_identical(q$skipped, 1L)
})

test_that("an abandoned execution releases everything", {
  calls <- local_replay("sql-query", responses = list(`5` = function(messages) {
    messages[[2]] <- as.raw(c(0xff, 0xff, 0xff))
    messages
  }))
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_error(spark_collect(q), class = "zuspark_protocol_error")
  expect_identical(calls$methods[[6]], "ReleaseExecute")
  expect_false(is.null(calls$requests[[6]]$release_all))
})

test_that("a stream that never progresses is a protocol error", {
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  release <- read_fixture("sql-query", fixture_calls("sql-query")$stem[[6]], "response")
  first <- read_stream_fixture("sql-query", fixture_calls("sql-query")$stem[[5]])[1]
  con$transport <- function(con, method, bytes, deadline, streaming = FALSE) {
    switch(method,
      ExecutePlan = replay_stream(first),
      ReattachExecute = replay_stream(list()),
      ReleaseExecute = release
    )
  }
  expect_error(spark_collect(q), "no progress", class = "zuspark_protocol_error")
  close_connection(con)
})

test_that("a failed release after ResultComplete keeps the result", {
  # Calls 4 and 6 are the releases after the SqlCommand's and the query's
  # ResultComplete. Both fail; the statement ran, so neither is an error.
  fail <- function(bytes) stop(grpc_transport_condition())
  calls <- local_replay("sql-query", responses = list("4" = fail, "6" = fail))
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_s3_class(q, "zuspark_query")
  df <- spark_collect(q)
  expect_identical(nrow(df), 5L)
  expect_identical(calls$methods[c(4, 6)], c("ReleaseExecute", "ReleaseExecute"))
  close_connection(con)
})

test_that("a response for another operation, or a result without its parts, is a protocol error", {
  sql <- "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)"
  without <- function(member) {
    function(messages) {
      keep <- vapply(messages, function(m) {
        is.null(zuproto::proto_decode(m, "spark.connect.ExecutePlanResponse", spark_pool())[[member]])
      }, logical(1))
      messages[keep]
    }
  }

  # A response on the query's stream (call 5) for another operation.
  local_replay("sql-query", responses = list(`5` = function(messages) {
    edit_response(messages, 1L, function(x) {
      x$operation_id <- "00000000-0000-4000-8000-000000000000"
      x
    })
  }))
  con <- replay_connect("sql-query")
  q <- spark_sql(con, sql)
  expect_error(spark_collect(q), class = "zuspark_protocol_error")
  close_connection(con)

  # The SqlCommand (call 3) answered without its SqlCommandResult.
  local_replay("sql-query", responses = list(`3` = without("sql_command_result")))
  con <- replay_connect("sql-query")
  expect_error(spark_sql(con, sql), class = "zuspark_protocol_error")
  close_connection(con)

  # The query (call 5) finished without a single batch.
  local_replay("sql-query", responses = list(`5` = without("arrow_batch")))
  con <- replay_connect("sql-query")
  q <- spark_sql(con, sql)
  expect_error(spark_collect(q), class = "zuspark_protocol_error")
  close_connection(con)
})
