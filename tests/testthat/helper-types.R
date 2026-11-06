# The type matrix of design §7.7: tests/testthat/fixtures/types/types.csv
# lists one query per type; tools/pyspark-types.py recorded PySpark's Arrow
# bytes for each (<name>.arrows), except the types PySpark itself cannot
# give as Arrow (unsupported.csv).

type_matrix <- function() {
  utils::read.csv(test_path("fixtures", "types", "types.csv"), stringsAsFactors = FALSE)
}

pyspark_refused <- function() {
  utils::read.csv(test_path("fixtures", "types", "unsupported.csv"), stringsAsFactors = FALSE)$name
}

pyspark_bytes <- function(name) {
  read_binpb(test_path("fixtures", "types", paste0(name, ".arrows")))
}

# The column `c` of an IPC stream, converted the way spark_collect() does.
convert_c <- function(bytes, lossy = "error", to = NULL) {
  arrow_call(zuarrow::arrow_to_r(read_batches(list(bytes)), to = to, lossy = lossy))$c
}

# Whether `package` has registered an exact target with zuarrow (zuint,
# decimal and zutempo had not yet on 2026-11-05: zuint#52, decimal#24).
registered_with_zuarrow <- function(package) {
  if (!requireNamespace(package, quietly = TRUE)) {
    return(FALSE)
  }
  package %in% zuarrow::arrow_registry()$package
}

# Data for test-types.R, kept here so that devtools::test(shuffle = TRUE)
# cannot run a test before it.
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
  name = c("bigint_large", "decimal", "interval_day_time"),
  package = c("zuint", "decimal", "zutempo"),
  text = c("9007199254740993", "12345678.91", "93784500000"),
  stringsAsFactors = FALSE
)

# Timestamps are zuarrow's to decide (D10), and its rule moved: zuarrow
# 0.1.0 as of 2026-11-04 errored without zutempo, as design §7.7 says;
# its main as of 2026-11-05 (825c43b) reads timestamp[us] as POSIXct.
# Either is accepted, at the instant PySpark recorded.
timestamps <- data.frame(
  name = c("timestamp", "timestamp_ntz"),
  text = c("2026-11-05T10:11:12.123456Z", "2026-11-05T10:11:12.123456"),
  stringsAsFactors = FALSE
)

instant <- as.POSIXct("2026-11-05 10:11:12.123456", tz = "UTC")
