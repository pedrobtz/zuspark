# Errors (design §8): every condition inherits from zuspark_error, and
# Spark's ErrorInfo reaches the caller as fields.

test_that("a Spark error is a zuspark_sql_error with Spark's fields", {
  cnd <- as_zuspark_error(spark_status_condition())
  expect_s3_class(cnd, c("zuspark_sql_error", "zuspark_error"))
  # zugrpc's classes are kept behind zuspark's.
  expect_s3_class(cnd, "zugrpc_error_internal")
  expect_identical(cnd$error_class, "TABLE_OR_VIEW_NOT_FOUND")
  expect_identical(cnd$sql_state, "42P01")
  expect_identical(cnd$message_parameters, c(relationName = "`no_such_table`"))
  expect_identical(cnd$classes[[1]], "org.apache.spark.sql.catalyst.ExtendedAnalysisException")
  expect_identical(cnd$error_id, "03dec9d6-18b5-422a-a050-f3c37925a892")
  expect_identical(cnd$grpc_status, "INTERNAL")
  expect_match(conditionMessage(cnd), "^\\[TABLE_OR_VIEW_NOT_FOUND\\]")
  expect_s3_class(cnd$parent, "zugrpc_error")
})

test_that("a status without Spark's ErrorInfo is a zuspark_rpc_error", {
  cnd <- as_zuspark_error(grpc_status_condition("UNAVAILABLE", "connection refused"))
  expect_s3_class(cnd, c("zuspark_rpc_error", "zuspark_error", "zugrpc_error_unavailable"))
  expect_identical(cnd$grpc_status, "UNAVAILABLE")
  cnd <- as_zuspark_error(grpc_transport_condition())
  expect_s3_class(cnd, c("zuspark_rpc_error", "zugrpc_error_transport"))
})

test_that("other conditions pass through", {
  e <- simpleError("x")
  expect_identical(as_zuspark_error(e), e)
})

test_that("a failing query raises the Spark error and releases its execution", {
  calls <- local_replay("error-details", responses = list(`3` = function(messages) {
    list(spark_status_condition(error_id = decode_fixture("error-details", 5, "request")$error_id))
  }))
  con <- replay_connect("error-details")
  err <- tryCatch(spark_sql(con, "SELECT * FROM no_such_table"), error = identity)
  expect_s3_class(err, "zuspark_sql_error")
  expect_identical(calls$methods[[4]], "ReleaseExecute")
  expect_false(is.null(calls$requests[[4]]$release_all))

  details <- spark_error_details(err)
  expect_identical(calls$requests[[5]]$error_id, err$error_id)
  expect_length(details, 1L)
  root <- details[[1]]
  expect_match(root$type[[1]], "AnalysisException")
  expect_identical(root$error_class, "TABLE_OR_VIEW_NOT_FOUND")
  expect_identical(root$sql_state, "42P01")
  expect_s3_class(root$stack, "data.frame")
  expect_gt(nrow(root$stack), 0L)
  expect_identical(root$contexts$fragment, "no_such_table")
  expect_true(is.na(root$cause))
  spark_disconnect(con)
})

test_that("error details need a Spark error with an id", {
  local_replay("connect")
  con <- replay_connect("connect")
  expect_error(spark_error_details(simpleError("x"), con), class = "zuspark_error")
  no_id <- as_zuspark_error(spark_status_condition(error_id = NA))
  expect_error(spark_error_details(no_id, con), "enrichError", class = "zuspark_error")
  # Built here rather than raised, it has no connection of its own.
  expect_null(no_id$con)
  close_connection(con)
})

test_that("a unary call's deadline reaches the transport, and its expiry is an rpc error", {
  local_replay("connect")
  con <- replay_connect("connect", deadline = 7)
  seen <- NULL
  con$transport <- function(con, method, bytes, deadline, streaming = FALSE) {
    seen <<- deadline
    stop(grpc_status_condition("DEADLINE_EXCEEDED", "deadline exceeded"))
  }
  err <- tryCatch(spark_config(con), error = identity)
  expect_identical(seen, 7)
  expect_s3_class(err, c("zuspark_rpc_error", "zugrpc_error_deadline_exceeded"))
  close_connection(con)
})

test_that("the default deadline is thirty seconds", {
  local_replay("connect")
  con <- replay_connect("connect")
  expect_identical(con$deadline, 30)
  close_connection(con)
})

test_that("Spark's JSON metadata parses", {
  expect_identical(parse_json_strings('["a","b"]'), c("a", "b"))
  expect_identical(parse_json_strings('{"k":"v","x":""}'), c(k = "v", x = ""))
  expect_identical(parse_json_strings('{"q":"say \\"hi\\"\\n","u":"\\u00e9"}'), c(q = "say \"hi\"\n", u = "é"))
  expect_identical(parse_json_strings(NA), character())
  expect_identical(parse_json_strings("[]"), character())
})

test_that("details the server no longer holds are an error, not an empty entry", {
  # The FetchErrorDetails answer (call 5) as the server gives it for an
  # error id it has dropped: the session, and no errors.
  recorded <- decode_fixture("error-details", 5)
  empty <- encode_response(
    list(session_id = recorded$session_id, server_side_session_id = recorded$server_side_session_id),
    "FetchErrorDetails"
  )
  local_replay("error-details", responses = list(
    `3` = function(messages) {
      list(spark_status_condition(error_id = decode_fixture("error-details", 5, "request")$error_id))
    },
    `5` = empty
  ))
  con <- replay_connect("error-details")
  err <- tryCatch(spark_sql(con, "SELECT * FROM no_such_table"), error = identity)
  gone <- tryCatch(spark_error_details(err), error = identity)
  expect_s3_class(gone, "zuspark_error")
  expect_identical(gone$error_id, err$error_id)
  close_connection(con)
})

test_that("a server without FetchErrorDetails keeps zugrpc's classes on its error", {
  local_replay("error-details", responses = list(
    `3` = function(messages) {
      list(spark_status_condition(error_id = decode_fixture("error-details", 5, "request")$error_id))
    },
    `5` = function(bytes) stop(grpc_status_condition("UNIMPLEMENTED", "Method not found"))
  ))
  con <- replay_connect("error-details")
  err <- tryCatch(spark_sql(con, "SELECT * FROM no_such_table"), error = identity)
  gone <- tryCatch(spark_error_details(err), error = identity)
  expect_s3_class(gone, "zuspark_rpc_error")
  expect_s3_class(gone, "zugrpc_error_unimplemented")
  expect_match(conditionMessage(gone), "predates Spark 4.0", fixed = TRUE)
  close_connection(con)
})
