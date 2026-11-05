# Fetch a query's result

`spark_collect()` runs a query and returns its result as one data frame.
`spark_fetch()` runs it and returns the result as an Arrow stream, for
code that consumes Arrow: 'zuarrow', 'nanoarrow', or `arrow_to_r()` with
its own conversion options. Each call runs the query again on the
server. [`as.data.frame()`](https://rdrr.io/r/base/as.data.frame.html)
on a query is `spark_collect()`.

## Usage

``` r
spark_collect(x, lossy = "error", to = NULL)

spark_fetch(x)

# S3 method for class 'zuspark_query'
as.data.frame(
  x,
  row.names = NULL,
  optional = FALSE,
  ...,
  lossy = "error",
  to = NULL
)
```

## Arguments

- x:

  A `zuspark_query` from
  [`spark_sql()`](https://pedrobtz.github.io/zuspark/reference/spark_sql.md).

- lossy, to:

  Passed to
  [`zuarrow::arrow_to_r()`](https://pedrobtz.github.io/zuarrow/reference/arrow_to_r.html):
  what to do with a column R cannot hold exactly, and prototypes for the
  columns.

- row.names, optional, ...:

  For [`as.data.frame()`](https://rdrr.io/r/base/as.data.frame.html):
  not used.

## Value

`spark_collect()` returns a data frame. `spark_fetch()` returns a
'zuarrow' stream handle, which is a 'nanoarrow' `ArrowArrayStream`.

## Details

Types reach R as 'zuarrow' converts them. A column R cannot hold
exactly, such as a `BIGINT` beyond 2^53 without 'zuint' loaded, is an
error by default; `lossy` chooses text or a widened type instead.

## Examples

``` r
if (FALSE) { # nzchar(Sys.getenv("SPARK_REMOTE"))
con <- spark_connect()
q <- spark_sql(con, "SELECT id FROM range(3)")
spark_collect(q)
stream <- spark_fetch(q)
zuarrow::arrow_to_r(stream)
spark_disconnect(con)
}
```
