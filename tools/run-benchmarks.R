# Measures design §10's targets against PySpark on the same machine and
# server, and prints the table for .agents/benchmarks.md.
#
#   Rscript tools/run-benchmarks.R sc://localhost:15002 /tmp/pyspark/bin/python
#
# The second argument is a Python with pyspark-client 4.2.x and pyarrow
# (see tools/benchmarks.py); without it, only zuspark's side is measured.
# The DuckDB row needs duckdb and arrow installed.

pkgload::load_all(quiet = TRUE)

args <- commandArgs(trailingOnly = TRUE)
remote <- if (length(args) >= 1L) args[[1]] else "sc://localhost:15002"
python <- if (length(args) >= 2L) args[[2]] else ""

median_time <- function(f, n) {
  stats::median(vapply(seq_len(n), function(i) {
    t <- proc.time()[["elapsed"]]
    f()
    proc.time()[["elapsed"]] - t
  }, numeric(1)))
}

# R heap allocated while `f` runs, in bytes above what it returns: gc()'s
# "max used" after a reset, less the result's own size.
heap_above_result <- function(f) {
  gc(reset = TRUE, full = TRUE)
  before <- sum(gc(full = TRUE)[, "used"] * c(56, 8))
  result <- f()
  peak <- sum(gc()[, "max used"] * c(56, 8))
  list(result = result, above = peak - before - as.numeric(utils::object.size(result)))
}

wide <- "SELECT id, id * 2 AS a, CAST(id AS DOUBLE) AS b, id % 7 AS c FROM range(10000000)"

r <- list()
connect <- function() spark_disconnect(spark_connect(remote))
invisible(connect())
r$connect <- median_time(connect, 5)

con <- spark_connect(remote)
one <- function() spark_collect(spark_sql(con, "SELECT 1"))
invisible(one())
r$select1 <- median_time(one, 50)

q <- spark_sql(con, wide)
invisible(spark_collect(q))
r$collect <- median_time(function() spark_collect(q), 3)

mem <- heap_above_result(function() spark_collect(q))
r$collect_above <- mem$above
rm(mem)
# One batch: the server's spark.connect.grpc.arrow.maxBatchSize, 4 MiB by
# default, as Arrow bytes.
r$batch <- 4 * 1024^2

if (requireNamespace("duckdb", quietly = TRUE) && requireNamespace("arrow", quietly = TRUE)) {
  # nanoarrow registers arrow's as_record_batch_reader() method for its
  # streams when it loads, which zuarrow's import alone does not do.
  loadNamespace("nanoarrow")
  local <- DBI::dbConnect(duckdb::duckdb())
  duck <- heap_above_result(function() {
    reader <- arrow::as_record_batch_reader(spark_fetch(q))
    duckdb::duckdb_register_arrow(local, "wide", reader)
    DBI::dbGetQuery(local, "SELECT count(*) AS n, sum(b) AS s FROM wide")
  })
  r$duckdb_rows <- duck$result$n
  r$duckdb_above <- duck$above
  DBI::dbDisconnect(local, shutdown = TRUE)
}
spark_disconnect(con)

py <- NULL
if (nzchar(python)) {
  out <- system2(python, c("-I", "tools/benchmarks.py", remote), stdout = TRUE)
  py <- jsonlite::fromJSON(out[[length(out)]])
}

ms <- function(s) sprintf("%.1f ms", 1000 * s)
mb <- function(b) sprintf("%.1f MB", b / 1e6)
ratio <- function(a, b) if (is.null(b)) "" else sprintf("%.2f×", a / b)

cat(sprintf("Measured %s, R %s, zuspark %s, Spark %s, PySpark %s.\n\n",
  format(Sys.Date()), getRversion(), utils::packageVersion("zuspark"),
  "4.2.1", py$pyspark %||% "not run"
))
cat("| Operation | Target | zuspark | PySpark | Ratio |\n| --- | --- | --- | --- | --- |\n")
cat(sprintf("| `spark_connect()` | under 100 ms | %s | %s | %s |\n", ms(r$connect), if (is.null(py)) "" else ms(py$connect), ratio(r$connect, py$connect)))
cat(sprintf("| `SELECT 1`, round trip | within 2× | %s | %s | %s |\n", ms(r$select1), if (is.null(py)) "" else ms(py$select1), ratio(r$select1, py$select1)))
cat(sprintf("| 10⁷ × 4, `spark_collect()` vs `toArrow()` | within 1.5× | %s | %s | %s |\n", ms(r$collect), if (is.null(py)) "" else ms(py$collect), ratio(r$collect, py$collect)))
if (!is.null(py)) {
  cat(sprintf("| 10⁷ × 4, `spark_collect()` vs `toPandas()` | (context) | %s | %s | %s |\n", ms(r$collect), ms(py$collect_pandas), ratio(r$collect, py$collect_pandas)))
}
if (!is.null(r$duckdb_above)) {
  cat(sprintf("| the same into DuckDB through `arrow` | no R-heap copy | %s on the R heap (%s rows) | | |\n", mb(r$duckdb_above), format(r$duckdb_rows)))
}
cat(sprintf("| R heap above the result, `spark_collect()` | under one batch (%s) | %s | | |\n", mb(r$batch), mb(r$collect_above)))
