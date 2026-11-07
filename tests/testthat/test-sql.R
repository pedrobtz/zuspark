# SQL (design §7.3, §7.5), replayed from tests/testthat/fixtures/sql-*.

test_that("a query comes back unexecuted, then collects", {
  calls <- local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_s3_class(q, "zuspark_query")
  expect_null(q$rows)
  expect_identical(names(q$relation), "sql")

  df <- spark_collect(q)
  expect_identical(df, data.frame(id = as.numeric(0:4), twice = as.numeric(0:4 * 2), s = as.character(0:4)))
  expect_identical(q$rows, 5)
  expect_false(is.null(q$metrics))
  spark_disconnect(con)
  expect_identical(
    calls$methods,
    c("Config", "AnalyzePlan", "ExecutePlan", "ReleaseExecute", "ExecutePlan", "ReleaseExecute", "ReleaseSession")
  )
})

test_that("the SQL command carries both input and the deprecated sql (Q2)", {
  calls <- local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  cmd <- calls$requests[[3]]$plan$command$sql_command
  expect_identical(cmd$sql, q$sql)
  expect_identical(cmd$input$sql$query, q$sql)
  close_connection(con)
})

test_that("every execution is reattachable, with its own operation id", {
  calls <- local_replay("sql-query")
  con <- replay_connect("sql-query")
  spark_collect(spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)"))
  spark_disconnect(con)
  executes <- calls$requests[calls$methods == "ExecutePlan"]
  for (req in executes) {
    expect_true(req$request_options[[1]]$reattach_options$reattachable)
    expect_match(req$operation_id, uuid_pattern)
  }
  expect_false(identical(executes[[1]]$operation_id, executes[[2]]$operation_id))
  # The second execution runs the relation the first returned.
  expect_identical(executes[[2]]$plan$root, decode_stream_fixture("sql-query", 3)[[1]]$sql_command_result$relation)

  releases <- calls$requests[calls$methods == "ReleaseExecute"]
  expect_identical(
    vapply(releases, `[[`, "", "operation_id"),
    vapply(executes, `[[`, "", "operation_id")
  )
  for (req in releases) expect_false(is.null(req$release_all))
})

test_that("the requests have the recorded shape", {
  calls <- local_replay("sql-query")
  con <- replay_connect("sql-query")
  spark_collect(spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)"))
  spark_disconnect(con)
  for (n in seq_along(calls$requests)) {
    sent <- calls$requests[[n]]
    recorded <- decode_fixture("sql-query", n, "request")
    sent$client_type <- recorded$client_type <- NULL
    sent$operation_id <- recorded$operation_id <- NULL
    expect_identical(sent, recorded, label = sprintf("request %d", n))
  }
})

test_that("a command's rows come back at once", {
  calls <- local_replay("sql-command")
  con <- replay_connect("sql-command")
  expect_identical(spark_sql(con, "SHOW DATABASES"), data.frame(namespace = "default"))
  view <- spark_sql(con, "CREATE OR REPLACE TEMPORARY VIEW zuspark_v AS SELECT 1 AS x")
  expect_s3_class(view, "data.frame")
  expect_identical(nrow(view), 0L)
  spark_disconnect(con)
  expect_identical(sum(calls$methods == "ExecutePlan"), 2L)
})

test_that("an empty result keeps its columns", {
  local_replay("sql-empty")
  con <- replay_connect("sql-empty")
  df <- spark_collect(spark_sql(con, "SELECT id FROM range(3) WHERE id > 99"))
  expect_identical(names(df), "id")
  expect_identical(nrow(df), 0L)
  spark_disconnect(con)
})

test_that("spark_fetch() returns a zuarrow stream of the same rows", {
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  stream <- spark_fetch(q)
  expect_s3_class(stream, "nanoarrow_array_stream")
  expect_identical(zuarrow::arrow_to_r(stream)$s, as.character(0:4))
  spark_disconnect(con)
})

test_that("as.data.frame() on a query is spark_collect()", {
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_identical(as.data.frame(q)$twice, as.numeric(0:4 * 2))
  spark_disconnect(con)
})

test_that("spark_execute() runs and counts the rows", {
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  expect_identical(
    withVisible(spark_execute(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")),
    list(value = 5, visible = FALSE)
  )
  spark_disconnect(con)
})

test_that("a query prints its SQL and its last run", {
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_output(print(q), "not run yet")
  spark_collect(q)
  expect_output(print(q), "last run: 5 rows")
  spark_disconnect(con)
})

test_that("SQL and queries are checked", {
  local_replay("connect")
  con <- replay_connect("connect")
  expect_error(spark_sql(con, ""), "non-empty", class = "zuspark_error")
  expect_error(spark_sql(con, c("a", "b")), class = "zuspark_error")
  expect_error(spark_collect(data.frame()), "already a data frame", class = "zuspark_error")
  close_connection(con)
})

test_that("the encoded plans are stable", {
  plan <- sql_command_plan("SELECT id FROM range(5)")
  bytes <- zuproto::proto_encode(plan, "spark.connect.Plan", spark_pool())
  expect_snapshot({
    cat(paste(format(as.hexmode(as.integer(bytes)), width = 2), collapse = ""), "\n")
    cat(zuproto::proto_to_json(bytes, "spark.connect.Plan", spark_pool()), "\n")
  })

  relation <- decode_stream_fixture("sql-query", 3)[[1]]$sql_command_result$relation
  bytes <- zuproto::proto_encode(list(root = relation), "spark.connect.Plan", spark_pool())
  expect_snapshot({
    cat(paste(format(as.hexmode(as.integer(bytes)), width = 2), collapse = ""), "\n")
    cat(zuproto::proto_to_json(bytes, "spark.connect.Plan", spark_pool()), "\n")
  })
})

test_that("spark_execute() counts a batch without row_count as no rows", {
  local_replay("sql-query", responses = list(`5` = function(messages) {
    for (i in seq_along(messages)) {
      messages <- edit_response(messages, i, function(x) {
        if (!is.null(x$arrow_batch)) x$arrow_batch$row_count <- NULL
        x
      })
    }
    messages
  }))
  con <- replay_connect("sql-query")
  expect_identical(spark_execute(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)"), 0)
  close_connection(con)
})
