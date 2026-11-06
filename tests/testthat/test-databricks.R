# Databricks (design §7.8): spark_connect_databricks() builds the
# connection string and the compute header, and the `databricks` scenario,
# recorded from a Free Edition workspace on serverless compute, replays.

test_that("serverless sends the session id in x-databricks-session-id", {
  seen <- local_connect_args()
  id <- "6f1d2c3b-4a5e-4f60-8a7b-9c0d1e2f3a4b"
  spark_connect_databricks("https://dbc-1.cloud.databricks.com/?o=42#notebook", "dapi-x", session_id = id)
  expect_identical(seen$url, "sc://dbc-1.cloud.databricks.com:443/;use_ssl=true")
  expect_identical(seen$args$token, "dapi-x")
  expect_identical(seen$args$session_id, id)
  expect_identical(seen$args$headers, c("x-databricks-session-id" = id))
})

test_that("serverless makes a session id when none is given", {
  seen <- local_connect_args()
  spark_connect_databricks("dbc-1.cloud.databricks.com", "dapi-x")
  id <- seen$args$session_id
  expect_match(id, uuid_pattern)
  expect_identical(seen$args$headers[["x-databricks-session-id"]], id)
})

test_that("a cluster id goes in x-databricks-cluster-id, next to other headers", {
  seen <- local_connect_args()
  spark_connect_databricks(
    "dbc-1.cloud.databricks.com:443", "dapi-x",
    cluster_id = "0123-456789-abcdefgh", headers = c(`x-extra` = "1"), user_agent = "app"
  )
  expect_identical(seen$url, "sc://dbc-1.cloud.databricks.com:443/;use_ssl=true")
  expect_null(seen$args$session_id)
  expect_identical(
    seen$args$headers,
    c(`x-extra` = "1", "x-databricks-cluster-id" = "0123-456789-abcdefgh")
  )
  expect_identical(seen$args$user_agent, "app")
})

test_that("the host and token default to DATABRICKS_HOST and DATABRICKS_TOKEN", {
  seen <- local_connect_args()
  withr::local_envvar(DATABRICKS_HOST = "https://dbc-2.cloud.databricks.com/", DATABRICKS_TOKEN = "dapi-y")
  spark_connect_databricks()
  expect_identical(seen$url, "sc://dbc-2.cloud.databricks.com:443/;use_ssl=true")
  expect_identical(seen$args$token, "dapi-y")
})

test_that("a missing host, token or bad cluster id is a url error", {
  withr::local_envvar(DATABRICKS_HOST = NA, DATABRICKS_TOKEN = NA)
  expect_error(spark_connect_databricks(token = "t"), class = "zuspark_url_error")
  expect_error(spark_connect_databricks("dbc-1.cloud.databricks.com"), class = "zuspark_url_error")
  expect_error(spark_connect_databricks("https://", "t"), class = "zuspark_url_error")
  expect_error(spark_connect_databricks("a b", "t"), class = "zuspark_url_error")
  expect_error(spark_connect_databricks("h", "t", cluster_id = 1), class = "zuspark_url_error")
  expect_error(spark_connect_databricks("h", "t", session_id = "nope"), class = "zuspark_url_error")
})

test_that("a serverless session recorded from a workspace replays", {
  # Call 8 failed with Spark's error in the gRPC status, which the fixture
  # holds no bytes for; the replay raises it as zugrpc would.
  calls <- local_replay("databricks", responses = list(
    "8" = function(messages) list(spark_status_condition(
      message = "[TABLE_OR_VIEW_NOT_FOUND] The table or view `zuspark_no_such_table` cannot be found.",
      parameters = "{\"relationName\":\"`zuspark_no_such_table`\"}"
    ))
  ))
  id <- fixture_session_id("databricks")
  con <- spark_connect_databricks(
    "dbc-00000000-0000.cloud.databricks.com", "<token>",
    session_id = id, user_id = "zuspark"
  )
  # Serverless reports the Spark version of its runtime.
  expect_identical(spark_version(con), "4.2.0")
  expect_identical(spark_session_id(con)[["client"]], id)

  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_identical(spark_schema(q)$type, c("bigint", "bigint", "string"))
  got <- spark_collect(q, lossy = "allow")
  expect_equal(as.numeric(got$twice), c(0, 2, 4, 6, 8))
  expect_identical(got$s, as.character(0:4))

  err <- tryCatch(spark_sql(con, "SELECT * FROM zuspark_no_such_table"), error = identity)
  expect_s3_class(err, "zuspark_sql_error")
  expect_identical(err$error_class, "TABLE_OR_VIEW_NOT_FOUND")

  expect_true(spark_disconnect(con))
  expect_identical(calls$methods[[length(calls$methods)]], "ReleaseSession")
  for (req in calls$requests) {
    expect_identical(req$session_id, id)
  }
})
