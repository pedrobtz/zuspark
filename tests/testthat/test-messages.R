# The wording of each condition family's messages (#47). Error paths are
# asserted on class elsewhere; these snapshots are where the text lives,
# so a change to it is a reviewable diff.

test_that("url errors", {
  expect_snapshot(error = TRUE, {
    parse_spark_url("sc://h/;token=%zz")
    parse_spark_url("sc://h:http")
    spark_connect("sc://h", user_agent = 1)
  })
})

test_that("session errors", {
  local_replay("connect")
  con <- replay_connect("connect")
  spark_disconnect(con)
  expect_snapshot(error = TRUE, spark_sql(con, "SELECT 1"))
})

test_that("version errors", {
  old <- decode_fixture("connect", 2)
  old$spark_version$version <- "3.4.1"
  local_replay("connect", responses = list(`2` = encode_response(old, "AnalyzePlan")))
  expect_snapshot(error = TRUE, replay_connect("connect"))
})

test_that("protocol errors", {
  local_replay("sql-query", responses = list(`5` = function(messages) {
    edit_response(messages, 1L, function(x) {
      x$operation_id <- "00000000-0000-4000-8000-000000000000"
      x
    })
  }))
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  # The operation id is the recorded one, so the text is stable.
  expect_snapshot(error = TRUE, spark_collect(q))
})

test_that("rpc and sql errors", {
  local_replay("connect", responses = list(`2` = function(bytes) {
    stop(grpc_status_condition("UNAVAILABLE", "connection refused"))
  }))
  expect_snapshot(error = TRUE, replay_connect("connect"))

  local_replay("error-details", responses = list(`3` = function(messages) {
    list(spark_status_condition(error_id = decode_fixture("error-details", 5, "request")$error_id))
  }))
  con <- replay_connect("error-details")
  expect_snapshot(error = TRUE, spark_sql(con, "SELECT * FROM no_such_table"))
})

test_that("interrupt errors", {
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  release <- read_fixture("sql-query", fixture_calls("sql-query")$stem[[6]], "response")
  con$transport <- function(con, method, bytes, deadline, streaming = FALSE) {
    switch(method,
      ExecutePlan = replay_stream(list(structure(
        class = c("zugrpc_error_interrupt", "zugrpc_error", "error", "condition"),
        list(message = "interrupted", call = NULL)
      ))),
      Interrupt = interrupt_response(con, character()),
      ReleaseExecute = release
    )
  }
  expect_snapshot(error = TRUE, spark_collect(q))
})
