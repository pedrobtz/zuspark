# Spark without the JVM

SparkR and sparklyr reach Spark through a local Spark installation and
its JVM; pysparklyr reaches Spark Connect through Python. zuspark needs
none of them. Spark Connect is gRPC carrying protocol buffers, with
results as Arrow record batches, and each of those has an R
implementation:

- **gRPC** through [zugrpc](https://github.com/pedrobtz/zugrpc), on
  [zuhttp](https://github.com/pedrobtz/zuhttp)’s HTTP/2 and the
  operating system’s TLS.
- **Protocol buffers** through
  [zuproto](https://github.com/pedrobtz/zuproto), which reads Spark’s
  message definitions at run time from a compiled descriptor set that
  zuspark ships, pinned to one Spark release.
- **Arrow** through [zuarrow](https://github.com/pedrobtz/zuarrow),
  which decodes the batches and converts them to R, refusing a value R
  cannot hold exactly unless you say otherwise.

zuspark itself is R code over these packages: it has no compiled code of
its own.
[`zuspark_info()`](https://pedrobtz.github.io/zuspark/reference/zuspark_info.md)
shows what it was built with.

``` r

library(zuspark)
zuspark_info()
#> zuspark 0.1.0, Spark Connect protocol v4.2.1
#>   descriptors: fba77546a88f7e96
#>   zuproto:     0.1.0
#>   zugrpc:      0.0.0.9000 on zuhttp 0.1.0
```

## What a query costs

A query is two round trips.
[`spark_sql()`](https://pedrobtz.github.io/zuspark/reference/spark_sql.md)
sends the statement and gets back the server’s plan for it, unexecuted;
[`spark_collect()`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)
or
[`spark_fetch()`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)
executes that plan and reads the result as it streams. The server keeps
the result buffered until the client says it is done with it, so that a
stream cut by a proxy or a timeout can be reattached where it stopped.
zuspark releases what it has read as it goes, and the rest when the
query finishes or fails.

``` r

con <- spark_connect()
q <- spark_sql(con, "SELECT id, id % 7 AS weekday, rand(42) AS amount FROM range(1000000)")
system.time(df <- spark_collect(q))
#>    user  system elapsed 
#>   0.279   0.045   0.882
nrow(df)
#> [1] 1000000
q
#> <zuspark_query> SELECT id, id % 7 AS weekday, rand(42) AS amount FROM range(1000000)
#>   on sc://127.0.0.1:15002 (last run: 1e+06 rows)
```

## A Spark result in DuckDB

[`spark_fetch()`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)
returns the result as an Arrow stream rather than a data frame. Code
that consumes Arrow can take it from there without the rows becoming R
vectors first. DuckDB is one such consumer: here a summary is computed
by Spark, and joined in DuckDB with a table that only exists locally.

DuckDB’s R package registers Arrow data through the arrow package, so
the stream is converted to an arrow `RecordBatchReader` first. Loading
nanoarrow provides that conversion.

``` r

library(DBI)
invisible(loadNamespace("nanoarrow"))

totals <- spark_sql(con, "
  SELECT id % 7 AS weekday, count(*) AS orders, sum(id % 100) / 100 AS total
  FROM range(1000000)
  GROUP BY id % 7
")

local <- dbConnect(duckdb::duckdb(shared_home = FALSE))
duckdb::duckdb_register_arrow(
  local, "spark_totals",
  arrow::as_record_batch_reader(spark_fetch(totals))
)

dbWriteTable(local, "days", data.frame(
  weekday = 0:6,
  name = c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
))

dbGetQuery(local, "
  SELECT d.name, t.orders, t.total
  FROM spark_totals t JOIN days d USING (weekday)
  ORDER BY weekday
")
#>   name orders    total
#> 1  Mon 142858 70714.71
#> 2  Tue 142857 70714.29
#> 3  Wed 142857 70713.86
#> 4  Thu 142857 70714.43
#> 5  Fri 142857 70714.00
#> 6  Sat 142857 70714.57
#> 7  Sun 142857 70714.14
dbDisconnect(local, shutdown = TRUE)
```

The batches arrive in R as Arrow bytes, and DuckDB reads them from
there; they are never converted to R vectors. They are on the R heap as
bytes for as long as the stream lives, though: decoding each batch as it
arrives is planned for a later version.

``` r

spark_disconnect(con)
```
