# The type matrix against a live server (design §7.7, §9): zuspark must get
# the schema and values PySpark got from the same queries.

test_that("live: spark_schema() gives each type as Spark names it", {
  con <- local_live_connection()
  m <- type_matrix()
  for (i in seq_len(nrow(m))) {
    s <- spark_schema(spark_sql(con, m$sql[[i]]))
    expect_identical(s$name, "c", label = m$name[[i]])
    expect_identical(s$type, m$spark_type[[i]], label = m$name[[i]])
  }
})

test_that("live: the Arrow schema and values match PySpark's", {
  con <- local_live_connection()
  # As tools/pyspark-types.py set it; a timestamp carries the session zone.
  spark_config(con, spark.sql.session.timeZone = "UTC")
  m <- type_matrix()
  for (name in setdiff(m$name, pyspark_refused())) {
    q <- spark_sql(con, m$sql[[match(name, m$name)]])
    ours <- spark_fetch(q)
    theirs <- arrow_call(zuarrow::arrow_read(pyspark_bytes(name), lazy = TRUE))
    expect_true(
      zuarrow::arrow_equal(zuarrow::arrow_schema(ours), zuarrow::arrow_schema(theirs), metadata = FALSE),
      label = paste(name, "schema")
    )
    expect_identical(
      spark_collect(q, lossy = "character")$c,
      convert_c(pyspark_bytes(name), lossy = "character"),
      label = paste(name, "values")
    )
  }
})

test_that("live: the interval types PySpark refuses have no R form", {
  con <- local_live_connection()
  m <- type_matrix()
  for (name in pyspark_refused()) {
    q <- spark_sql(con, m$sql[[match(name, m$name)]])
    expect_error(spark_collect(q, lossy = "character"), class = "zuarrow_lossy_error", label = name)
  }
})

test_that("live: Spark 4.2.1 refuses to send TIME as Arrow", {
  con <- local_live_connection()
  q <- spark_sql(con, "SELECT TIME '10:11:12' AS c")
  expect_identical(spark_schema(q)$type, "time(6)")
  err <- tryCatch(spark_collect(q), error = identity)
  expect_s3_class(err, "zuspark_sql_error")
  expect_identical(err$error_class, "UNSUPPORTED_TIME_TYPE")
})
