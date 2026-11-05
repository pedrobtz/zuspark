# Errors and interrupts against a live server (design §7.9, §8).

test_that("live: a typo gives Spark's class, SQLSTATE and parameters", {
  con <- local_live_connection()
  err <- tryCatch(spark_sql(con, "SELEC 1"), error = identity)
  expect_s3_class(err, "zuspark_sql_error")
  expect_identical(err$error_class, "PARSE_SYNTAX_ERROR")
  expect_identical(err$sql_state, "42601")
  expect_identical(err$message_parameters[["error"]], "'SELEC'")
  expect_contains(err$classes, "org.apache.spark.sql.AnalysisException")
})

test_that("live: a missing table, and its details", {
  con <- local_live_connection()
  err <- tryCatch(spark_collect(spark_sql(con, "SELECT * FROM no_such_table")), error = identity)
  expect_identical(err$error_class, "TABLE_OR_VIEW_NOT_FOUND")
  expect_identical(err$sql_state, "42P01")
  details <- spark_error_details(err)
  expect_identical(details[[1]]$error_class, "TABLE_OR_VIEW_NOT_FOUND")
  expect_identical(details[[1]]$contexts$fragment, "no_such_table")
})

test_that("live: an interrupt during range(10^9) stops the server job", {
  skip_on_os("windows")
  con <- local_live_connection()
  q <- spark_sql(con, "SELECT sum(id) FROM range(1000000000) GROUP BY id % 7")
  # A real SIGINT, three seconds in, as Ctrl-C would send it.
  system(sprintf("sleep 3 && kill -INT %d", Sys.getpid()), wait = FALSE)
  elapsed <- system.time(err <- tryCatch(spark_collect(q), error = identity))[["elapsed"]]
  expect_s3_class(err, "zuspark_interrupt_error")
  expect_identical(err$interrupted_ids, q$operation_id)
  expect_lt(elapsed, 30)
  # The session stays usable.
  expect_identical(spark_collect(spark_sql(con, "SELECT 1 AS one"))$one, 1L)
})
