# spark_schema() (design §6, decided 2026-11-05): Spark's schema through
# AnalyzePlan, as DDL text.

schema_sql <- paste(
  "SELECT 1L AS id, CAST(1.5 AS DECIMAL(10, 2)) AS amount,",
  "array(named_struct('k', 'a', 'v', map('x', 1))) AS nested,",
  "INTERVAL '1' HOUR AS h, CAST(NULL AS STRING) AS maybe"
)

test_that("the schema comes from AnalyzePlan, without running the query", {
  calls <- local_replay("schema")
  con <- replay_connect("schema")
  s <- spark_schema(spark_sql(con, schema_sql))
  expect_identical(
    s,
    data.frame(
      name = c("id", "amount", "nested", "h", "maybe"),
      type = c(
        "bigint", "decimal(10,2)", "array<struct<k:string,v:map<string,int>>>",
        "interval hour", "string"
      ),
      nullable = c(FALSE, FALSE, FALSE, FALSE, TRUE)
    )
  )
  spark_disconnect(con)
  expect_identical(calls$methods[[5]], "AnalyzePlan")
  expect_false(is.null(calls$requests[[5]]$schema$plan$root))
  expect_false("ExecutePlan" %in% calls$methods[-3])
})

test_that("DDL text covers Spark's type kinds", {
  dt <- function(kind, ...) stats::setNames(list(list(...)), kind)
  expect_identical(format_data_type(dt("null")), "void")
  expect_identical(format_data_type(dt("byte")), "tinyint")
  expect_identical(format_data_type(dt("decimal", precision = 38, scale = 18)), "decimal(38,18)")
  expect_identical(format_data_type(dt("decimal")), "decimal(10,0)")
  expect_identical(format_data_type(dt("string", collation = "UTF8_LCASE")), "string collate UTF8_LCASE")
  expect_identical(format_data_type(dt("string", collation = "UTF8_BINARY")), "string")
  expect_identical(format_data_type(dt("char", length = 3)), "char(3)")
  expect_identical(format_data_type(dt("var_char", length = 9)), "varchar(9)")
  expect_identical(format_data_type(dt("time", precision = 3)), "time(3)")
  expect_identical(format_data_type(dt("day_time_interval")), "interval day to second")
  expect_identical(
    format_data_type(dt("day_time_interval", start_field = 1, end_field = 2)),
    "interval hour to minute"
  )
  expect_identical(format_data_type(dt("year_month_interval", start_field = 1, end_field = 1)), "interval month")
  expect_identical(format_data_type(dt("calendar_interval")), "interval")
  expect_identical(
    format_data_type(dt("map", key_type = dt("string"), value_type = dt("array", element_type = dt("long")))),
    "map<string,array<bigint>>"
  )
  expect_identical(format_data_type(dt("udt", sql_type = dt("double"))), "double")
  expect_identical(format_data_type(dt("unparsed", data_type_string = "geo")), "geo")
  expect_identical(format_data_type(list()), "unknown")
})

test_that("spark_schema() needs a query", {
  expect_error(spark_schema(data.frame()), class = "zuspark_error")
})
