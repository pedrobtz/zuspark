# The type edge (design §7.7, D10): zuspark adds nothing to zuarrow's rule.
# These run PySpark's recorded bytes through the conversion spark_collect()
# makes, on every leg.

exact <- list(
  boolean = c(TRUE, NA),
  tinyint = c(-7L, NA),
  smallint = c(300L, NA),
  int = c(2147483647L, NA),
  bigint_small = c(2^53, NA),
  float = c(1.5, NA),
  double = c(0.1, NA),
  string = c("naïve ✓", NA),
  date = as.Date(c("2026-11-05", NA)),
  null = NA
)

lossy <- data.frame(
  name = c("bigint_large", "decimal", "timestamp", "timestamp_ntz", "interval_day_time"),
  package = c("zuint", "decimal", "zutempo", "zutempo", "zutempo"),
  text = c(
    "9007199254740993", "12345678.91", "2026-11-05T10:11:12.123456Z",
    "2026-11-05T10:11:12.123456", "93784500000"
  ),
  stringsAsFactors = FALSE
)

test_that("PySpark's bytes cover the matrix, less what PySpark refuses", {
  m <- type_matrix()
  recorded <- sub("[.]arrows$", "", list.files(test_path("fixtures", "types"), "[.]arrows$"))
  expect_setequal(c(recorded, pyspark_refused()), m$name)
  expect_setequal(pyspark_refused(), c("interval_year_month", "interval_calendar"))
})

test_that("types R holds exactly convert under every lossy policy", {
  for (name in names(exact)) {
    for (policy in c("error", "character", "allow")) {
      expect_identical(
        convert_c(pyspark_bytes(name), lossy = policy),
        exact[[name]],
        label = sprintf("%s, lossy = %s", name, policy)
      )
    }
  }
})

test_that("nested and binary types become lists and data frames", {
  expect_identical(convert_c(pyspark_bytes("binary")), list(as.raw(c(0x00, 0xff, 0x10)), NULL))
  expect_identical(convert_c(pyspark_bytes("array")), list(c(1L, NA, 3L), NULL))
  st <- convert_c(pyspark_bytes("struct"))
  expect_s3_class(st, "data.frame")
  expect_identical(st$x, c(1L, NA))
  expect_identical(st$y, c("z", NA))
  m <- convert_c(pyspark_bytes("map"))[[1]]
  expect_identical(m$key, c("a", "b"))
  expect_identical(m$value, c(1L, 2L))
  # VARIANT arrives as its two binary fields, undecoded (§7.7).
  v <- convert_c(pyspark_bytes("variant"))
  expect_identical(names(v), c("value", "metadata"))
  expect_type(v$value, "list")
})

test_that("types without an exact R form error by default, naming the package", {
  for (i in seq_len(nrow(lossy))) {
    if (registered_with_zuarrow(lossy$package[[i]])) next
    expect_error(
      convert_c(pyspark_bytes(lossy$name[[i]])),
      lossy$package[[i]],
      class = "zuarrow_lossy_error",
      label = lossy$name[[i]]
    )
  }
})

test_that("lossy = 'character' gives their exact text", {
  for (i in seq_len(nrow(lossy))) {
    got <- convert_c(pyspark_bytes(lossy$name[[i]]), lossy = "character")
    expect_identical(as.vector(got), c(lossy$text[[i]], NA), label = lossy$name[[i]])
  }
})

test_that("lossy = 'allow' widens them", {
  expect_identical(convert_c(pyspark_bytes("bigint_large"), lossy = "allow"), c(9007199254740992, NA))
  expect_identical(convert_c(pyspark_bytes("decimal"), lossy = "allow"), c(12345678.91, NA))
  ts <- convert_c(pyspark_bytes("timestamp"), lossy = "allow")
  expect_s3_class(ts, "POSIXct")
  expect_equal(as.numeric(ts[[1]]), as.numeric(as.POSIXct("2026-11-05 10:11:12.123456", tz = "UTC")))
  expect_s3_class(convert_c(pyspark_bytes("interval_day_time"), lossy = "allow"), "difftime")
})

test_that("with the exact packages registered, those types convert exactly", {
  for (i in seq_len(nrow(lossy))) {
    pkg <- lossy$package[[i]]
    skip_if_not(registered_with_zuarrow(pkg), sprintf("%s does not register with zuarrow yet", pkg))
    expect_no_error(convert_c(pyspark_bytes(lossy$name[[i]])))
  }
})

test_that("`to` and `lossy` pass through spark_collect()", {
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  df <- spark_collect(q, to = list(id = integer(), twice = integer(), s = character()))
  expect_identical(df$id, 0:4)
  spark_disconnect(con)
})
