# The rest of design §7 and §8 against a live server: results in many
# batches, metrics, interrupts with nothing running, deadlines, transport
# failures and released sessions.

test_that("live: a result in many batches arrives whole, with its metrics", {
  con <- local_live_connection()
  # 4 x 8-byte columns over 3,000,000 rows is about 96 MB, many times the
  # server's 4 MiB batch size.
  q <- spark_sql(con, "SELECT id, id * 2 AS a, CAST(id AS DOUBLE) AS b, id % 7 AS c FROM range(3000000)")
  df <- spark_collect(q)
  expect_identical(nrow(df), 3000000L)
  expect_identical(q$rows, 3e6)
  expect_identical(sum(df$id), (3e6 - 1) * 3e6 / 2)
  expect_identical(df$a[[3000000]], 2 * (3e6 - 1))
  expect_false(is.null(q$metrics))
  expect_identical(q$skipped, 0L)

  stream <- spark_fetch(q)
  expect_identical(nrow(zuarrow::arrow_to_r(stream)), 3000000L)
})

test_that("live: interrupting with nothing running interrupts nothing", {
  con <- local_live_connection()
  expect_identical(spark_interrupt(con), character())
  q <- spark_sql(con, "SELECT 1 AS x")
  # Not run yet: no operation to name.
  expect_identical(spark_interrupt(con, q), character())
  spark_collect(q)
  expect_match(q$operation_id, uuid_pattern)
  expect_identical(spark_interrupt(con, q), character())
})

test_that("live: a deadline too short for a call is a zuspark_rpc_error", {
  con <- local_live_connection()
  err <- tryCatch(
    interrupt_rpc(con, deadline = 1e-6),
    error = identity
  )
  expect_s3_class(err, "zuspark_rpc_error")
  expect_s3_class(err, "zugrpc_error_deadline_exceeded")
  # The connection is still usable after a call ran out of time.
  expect_identical(spark_collect(spark_sql(con, "SELECT 2 AS x"))$x, 2L)
})

test_that("live: no server on the port is a transport error", {
  remote <- live_remote()
  host <- sub("^sc://([^:/]+).*$", "\\1", remote)
  err <- tryCatch(spark_connect(sprintf("sc://%s:1", host), deadline = 5), error = identity)
  expect_s3_class(err, "zuspark_rpc_error")
  expect_s3_class(err, "zugrpc_error_transport")
})

test_that("live: a released session refuses every verb", {
  con <- spark_connect(live_remote())
  q <- spark_sql(con, "SELECT 1 AS x")
  expect_true(spark_disconnect(con))
  expect_error(spark_sql(con, "SELECT 1"), class = "zuspark_session_error")
  expect_error(spark_collect(q), class = "zuspark_session_error")
  expect_error(spark_config(con, "spark.app.name"), class = "zuspark_session_error")
  expect_error(spark_interrupt(con), class = "zuspark_session_error")
  expect_false(spark_disconnect(con))
})
