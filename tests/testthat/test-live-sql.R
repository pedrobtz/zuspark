# SQL against a live server (design §7.3 to §7.5, §9).

test_that("live: a query collects, fetches and runs again each time", {
  con <- local_live_connection()
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_s3_class(q, "zuspark_query")
  df <- spark_collect(q)
  expect_identical(df$twice, as.numeric(0:4 * 2))
  expect_identical(zuarrow::arrow_to_r(spark_fetch(q))$s, as.character(0:4))
  expect_identical(spark_execute(con, "SELECT * FROM range(1000)"), 1000)
})

test_that("live: commands return their rows at once", {
  con <- local_live_connection()
  expect_contains(spark_sql(con, "SHOW DATABASES")$namespace, "default")
  spark_sql(con, "CREATE OR REPLACE TEMPORARY VIEW zuspark_live AS SELECT 7 AS x")
  expect_identical(spark_collect(spark_sql(con, "SELECT x FROM zuspark_live"))$x, 7L)
  spark_sql(con, "DROP VIEW zuspark_live")
})

test_that("live: an empty result keeps its columns", {
  con <- local_live_connection()
  df <- spark_collect(spark_sql(con, "SELECT id, 'a' AS s FROM range(3) WHERE id > 99"))
  expect_identical(names(df), c("id", "s"))
  expect_identical(nrow(df), 0L)
})

test_that("live: lossy columns follow zuarrow's rule", {
  con <- local_live_connection()
  q <- spark_sql(con, "SELECT 9007199254740993 AS big")
  expect_error(spark_collect(q), class = "zuarrow_lossy_error")
  big <- spark_collect(q, lossy = "character")$big
  # zuarrow records the Arrow type the text came from; zuspark keeps it (D10).
  expect_identical(attr(big, "arrow_type"), "l")
  expect_identical(as.vector(big), "9007199254740993")
})

test_that("live: range(10^8) through a server that cuts its streams, every row once", {
  remote <- Sys.getenv("ZUSPARK_TEST_REMOTE_CUT")
  skip_if(!nzchar(remote), "ZUSPARK_TEST_REMOTE_CUT is not set")
  con <- spark_connect(remote)
  withr::defer(spark_disconnect(con))
  q <- spark_sql(con, "SELECT id FROM range(100000000)")
  df <- spark_collect(q)
  expect_identical(nrow(df), 100000000L)
  expect_gte(q$reattaches, 1L)
  expect_identical(sum(df$id), (1e8 - 1) * 1e8 / 2)
  expect_identical(anyDuplicated(df$id), 0L)
})
