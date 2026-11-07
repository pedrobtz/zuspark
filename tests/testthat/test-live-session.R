# The session tests of test-session.R, against a live server (design §9).

test_that("live: connect, version and session ids", {
  con <- local_live_connection()
  expect_s3_class(con, "zuspark_connection")
  expect_identical(spark_version(con), "4.2.1")
  ids <- spark_session_id(con)
  expect_match(ids[["client"]], uuid_pattern)
  expect_match(ids[["server"]], uuid_pattern)
})

test_that("live: read, set, unset and restore settings", {
  con <- local_live_connection()
  all <- spark_config(con)
  expect_contains(names(all), "spark.app.name")

  zone <- spark_config(con, "spark.sql.session.timeZone")
  old <- spark_config(con, spark.sql.session.timeZone = "UTC", zuspark.test.key = "1")
  expect_identical(old, c(spark.sql.session.timeZone = zone[[1]], zuspark.test.key = NA))
  expect_identical(
    spark_config(con, "spark.sql.session.timeZone", "zuspark.test.key"),
    c(spark.sql.session.timeZone = "UTC", zuspark.test.key = "1")
  )
  do.call(spark_config, c(list(con), as.list(old)))
  expect_identical(
    spark_config(con, "spark.sql.session.timeZone", "zuspark.test.key"),
    c(spark.sql.session.timeZone = zone[[1]], zuspark.test.key = NA)
  )
})

test_that("live: a released session id is retired unless reconnects are allowed", {
  remote <- live_remote()
  con <- spark_connect(remote)
  withr::defer(if (!isTRUE(con$released)) spark_disconnect(con))
  id <- spark_session_id(con)[["client"]]
  expect_true(spark_disconnect(con))
  # Spark answers INTERNAL with INVALID_HANDLE.SESSION_CLOSED.
  err <- tryCatch(spark_connect(remote, session_id = id), error = identity)
  expect_s3_class(err, "zuspark_sql_error")
  expect_identical(err$error_class, "INVALID_HANDLE.SESSION_CLOSED")

  con2 <- spark_connect(remote)
  withr::defer(if (!isTRUE(con2$released)) spark_disconnect(con2))
  id <- spark_session_id(con2)[["client"]]
  expect_true(spark_disconnect(con2, allow_reconnect = TRUE))
  again <- spark_connect(remote, session_id = id)
  withr::defer(if (!isTRUE(again$released)) spark_disconnect(again))
  expect_identical(spark_session_id(again)[["client"]], id)
  spark_disconnect(again)
})

test_that("live: headers from the string reach the server without breaking calls", {
  con <- local_live_connection(`x-zuspark-test` = "1")
  expect_identical(spark_version(con), "4.2.1")
})
