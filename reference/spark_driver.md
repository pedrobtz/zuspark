# A DBI driver for Spark Connect

`spark_driver()` returns the driver to give
[`DBI::dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html), so
that a Spark Connect session can be used through DBI and dbplyr:
[`DBI::dbGetQuery()`](https://dbi.r-dbi.org/reference/dbGetQuery.html),
[`DBI::dbListTables()`](https://dbi.r-dbi.org/reference/dbListTables.html),
[`dplyr::tbl()`](https://dplyr.tidyverse.org/reference/tbl.html),
[`dplyr::collect()`](https://dplyr.tidyverse.org/reference/compute.html),
[`dplyr::copy_to()`](https://dplyr.tidyverse.org/reference/copy_to.html)
and the rest. It needs the DBI package; dbplyr, for
[`tbl()`](https://dplyr.tidyverse.org/reference/tbl.html), uses its own
'Spark SQL' translation.

## Usage

``` r
spark_driver()
```

## Value

A `ZusparkDriver` object.

## Details

`dbConnect(spark_driver(), url, ...)` passes `url` and `...` to
[`spark_connect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md);
`dbConnect(spark_driver(), con = con)` wraps a connection made already,
such as one from
[`spark_connect_databricks()`](https://pedrobtz.github.io/zuspark/reference/spark_connect_databricks.md).
Every `spark_*()` function also accepts the DBI connection.

Results follow zuspark's types: a value R cannot hold exactly is an
error unless `lossy` says otherwise. Pass `lossy` to `dbGetQuery()` or
`dbSendQuery()`, or set `options(zuspark.lossy = "allow")` (or
`"character"`) for every DBI and dbplyr query, since many Spark tables
have `DECIMAL` or `BIGINT` columns. `dbFetch(res, n)` returns the rows
in chunks, but reads the whole result from the server on its first call.

`dbWriteTable()`, `dbAppendTable()` and
[`copy_to()`](https://dplyr.tidyverse.org/reference/copy_to.html) send
the rows as SQL literals, which suits small tables: they warn above
`getOption("zuspark.write_warn_rows", 10000)` rows. A temporary table is
a temporary view of the session. Spark has no transactions, so
`dbBegin()` is an error, and it does not report rows affected, so
`dbExecute()` returns `NA`.

## Examples

``` r
con <- DBI::dbConnect(spark_driver())
DBI::dbGetQuery(con, "SELECT id, id * 2 AS twice FROM range(3)")
#>   id twice
#> 1  0     0
#> 2  1     2
#> 3  2     4
small <- data.frame(x = 1:3, y = c("a", "b", "c"))
DBI::dbWriteTable(con, "small", small, temporary = TRUE)
DBI::dbListTables(con)
#> [1] "small"
DBI::dbDisconnect(con)
```
