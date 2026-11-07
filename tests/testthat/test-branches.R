# Branches the review found untested (#46, #49): each replays a scenario
# with one response changed.

test_that("lossy reaches zuarrow from spark_collect() and as.data.frame()", {
  seen <- character()
  convert <- zuarrow::arrow_to_r
  local_mocked_bindings(
    arrow_to_r = function(x, ..., lossy = "error") {
      seen <<- c(seen, lossy)
      convert(x, ..., lossy = lossy)
    },
    .package = "zuarrow"
  )
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, query_sql())
  spark_collect(q, lossy = "character")
  expect_identical(seen, "character")
})

test_that("server warnings on a setting reach the caller as R warnings", {
  recorded <- decode_fixture("session", 3)
  recorded$warnings <- "The SQL config 'x' is deprecated."
  local_replay("session", responses = list(`3` = encode_response(recorded, "Config")))
  con <- replay_connect("session")
  expect_warning(
    spark_config(con, "spark.sql.session.timeZone", "zuspark.no.such.key"),
    "deprecated"
  )
})

test_that("settings to read must be strings, and reads and sets don't mix", {
  local_replay("connect")
  con <- replay_connect("connect")
  expect_error(spark_config(con, 1), class = "zuspark_error")
  expect_error(spark_config(con, NA_character_), class = "zuspark_error")
  expect_error(spark_config(con, "a", b = 1), class = "zuspark_error")
})

test_that("a schema answer without a schema is a protocol error", {
  recorded <- decode_fixture("schema", 5)
  recorded$schema <- NULL
  local_replay("schema", responses = list(`5` = encode_response(recorded, "AnalyzePlan")))
  con <- replay_connect("schema")
  q <- spark_sql(con, schema_sql)
  expect_error(spark_schema(q), class = "zuspark_protocol_error")
})

test_that("a schema that is not a struct is one column named value", {
  recorded <- decode_fixture("schema", 5)
  recorded$schema <- list(schema = list(long = list()))
  local_replay("schema", responses = list(`5` = encode_response(recorded, "AnalyzePlan")))
  con <- replay_connect("schema")
  q <- spark_sql(con, schema_sql)
  expect_identical(
    spark_schema(q),
    data.frame(name = "value", type = "bigint", nullable = TRUE)
  )
})

test_that("every DataType kind has its DDL name", {
  cases <- list(
    list(list(null = list()), "void"),
    list(list(binary = list()), "binary"),
    list(list(boolean = list()), "boolean"),
    list(list(byte = list()), "tinyint"),
    list(list(short = list()), "smallint"),
    list(list(float = list()), "float"),
    list(list(date = list()), "date"),
    list(list(timestamp = list()), "timestamp"),
    list(list(timestamp_ntz = list()), "timestamp_ntz"),
    list(list(time = list(precision = 3)), "time(3)"),
    list(list(char = list(length = 4)), "char(4)"),
    list(list(var_char = list(length = 9)), "varchar(9)"),
    list(list(string = list(collation = "UNICODE_CI")), "string collate UNICODE_CI"),
    list(list(calendar_interval = list()), "interval"),
    list(list(year_month_interval = list(start_field = 0, end_field = 1)), "interval year to month"),
    list(list(day_time_interval = list(start_field = 1, end_field = 1)), "interval hour"),
    list(list(variant = list()), "variant"),
    list(list(geometry = list(srid = 4326)), "geometry(4326)"),
    list(list(geography = list()), "geography(4326)"),
    list(list(udt = list(sql_type = list(double = list()))), "double"),
    list(list(udt = list()), "udt"),
    list(list(unparsed = list(data_type_string = "x")), "x"),
    list(list(map = list(key_type = list(string = list()), value_type = list(integer = list()))), "map<string,int>"),
    list(list(), "unknown")
  )
  for (case in cases) {
    expect_identical(format_data_type(case[[1]]), case[[2]], label = case[[2]])
  }
})

test_that("spark_execute() on a command returns its row count", {
  local_replay("sql-command")
  con <- replay_connect("sql-command")
  expect_identical(spark_execute(con, "SHOW DATABASES"), 1L)
})

test_that("a result in more than 16 batches is kept whole", {
  # The query's first batch, repeated 20 times with running offsets, in
  # place of the recorded batches.
  decoded <- decode_stream_fixture("sql-query", 5)
  is_batch <- vapply(decoded, function(r) !is.null(r$arrow_batch), logical(1))
  batch <- decoded[[which(is_batch)[[1L]]]]
  per_batch <- batch$arrow_batch$row_count
  local_replay("sql-query", responses = list(`5` = function(messages) {
    copies <- lapply(0:19, function(k) {
      b <- batch
      b$arrow_batch$start_offset <- per_batch * k
      b$response_id <- sprintf("%s-%02d", batch$response_id, k)
      encode_response(b, "ExecutePlan")
    })
    first <- which(is_batch)[[1L]]
    c(messages[seq_len(first - 1L)], copies, messages[!is_batch & seq_along(messages) > first])
  }))
  con <- replay_connect("sql-query")
  q <- spark_sql(con, query_sql())
  df <- spark_collect(q)
  expect_identical(nrow(df), as.integer(20 * per_batch))
  expect_identical(q$rows, 20 * per_batch)
})

test_that("observed metrics are kept on the query", {
  local_replay("sql-query", responses = list(`5` = function(messages) {
    edit_response(messages, 1L, function(x) {
      x$observed_metrics <- list(list(name = "rows_seen"))
      x
    })
  }))
  con <- replay_connect("sql-query")
  q <- spark_sql(con, query_sql())
  spark_collect(q)
  expect_length(q$observed_metrics, 1L)
  expect_identical(q$observed_metrics[[1]]$name, "rows_seen")
})
