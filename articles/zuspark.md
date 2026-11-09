# Get started with zuspark

Spark Connect lets a client run Spark SQL on a remote cluster over gRPC,
but every R route to it so far has gone through a JVM or a Python
environment. zuspark speaks the protocol itself, so all you need to
query a Spark Connect endpoint is R packages: connect, run SQL, and
bring the result back as Arrow or as a data frame.

``` r

library(zuspark)
```

## Connect

[`spark_connect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)
takes an `sc://` connection string, by default the one in the
`SPARK_REMOTE` environment variable. A local Spark Connect server
listens on port 15002; this article runs against one.

``` r

con <- spark_connect()
con
#> <zuspark_connection> sc://127.0.0.1:15002 (Spark 4.2.1)
#>   session: 4bea985c-b492-4b7e-9e72-990df029596c
spark_version(con)
#> [1] "4.2.1"
```

Parameters go after a `;`. `token` sends a bearer token and turns TLS
on; any parameter zuspark doesn’t recognise is sent as a request header.

``` r

con <- spark_connect("sc://spark.example.com:443/;token=my-token")
```

For Databricks,
[`spark_connect_databricks()`](https://pedrobtz.github.io/zuspark/reference/spark_connect_databricks.md)
builds the string and the header the workspace needs; see the
[Databricks
article](https://pedrobtz.github.io/zuspark/articles/databricks.md).

## Run SQL

[`spark_sql()`](https://pedrobtz.github.io/zuspark/reference/spark_sql.md)
sends a statement. A query comes back as a `zuspark_query`, which holds
the plan but no rows yet; nothing is read until you ask for it.

``` r

q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
q
#> <zuspark_query> SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)
#>   on sc://127.0.0.1:15002 (not run yet)
```

[`spark_schema()`](https://pedrobtz.github.io/zuspark/reference/spark_schema.md)
asks the server for the query’s types without running it. They are
Spark’s types, as Spark names them.

``` r

spark_schema(q)
#>    name   type nullable
#> 1    id bigint    FALSE
#> 2 twice bigint    FALSE
#> 3     s string    FALSE
```

Commands such as DDL, DML and `SET` run at once, and
[`spark_sql()`](https://pedrobtz.github.io/zuspark/reference/spark_sql.md)
returns the rows they produce as a data frame.

``` r

spark_sql(con, "CREATE OR REPLACE TEMPORARY VIEW small AS SELECT * FROM range(3)")
#> data frame with 0 columns and 0 rows
spark_sql(con, "SHOW VIEWS")
#>   namespace viewName isTemporary
#> 1              small        TRUE
```

## Collect the result

[`spark_collect()`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)
runs the query and returns one data frame. It runs it again every time,
so the result is always current.

``` r

spark_collect(q)
#>   id twice s
#> 1  0     0 0
#> 2  1     2 1
#> 3  2     4 2
#> 4  3     6 3
#> 5  4     8 4
```

Values that R can’t hold exactly, such as a `BIGINT` beyond 2^53, are an
error by default. `lossy` chooses their exact text or a widened type
instead.

``` r

big <- spark_sql(con, "SELECT 9007199254740993 AS id")
conditionMessage(tryCatch(spark_collect(big), error = identity))
#> [1] "Column `id` (Arrow format `l`) can't be converted to R exactly: row 1 holds 9007199254740993. Load zuint to read it exactly, or use `lossy = \"character\"` for its exact text, or `lossy = \"allow\"` to accept the loss."
spark_collect(big, lossy = "character")
#>                 id
#> 1 9007199254740993
spark_collect(big, lossy = "allow")
#>             id
#> 1 9.007199e+15
```

[`spark_fetch()`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)
returns the result as an Arrow stream instead, for code that consumes
Arrow directly, and
[`spark_execute()`](https://pedrobtz.github.io/zuspark/reference/spark_sql.md)
runs a statement only for its effect, returning the number of rows it
produced.

``` r

stream <- spark_fetch(q)
zuarrow::arrow_schema(stream)
#> <zuarrow_schema> struct<id: int64, twice: int64, s: string>
n <- spark_execute(con, "SELECT * FROM range(1000000)")
n
#> [1] 1e+06
```

## Errors

Spark’s errors arrive as a `zuspark_sql_error`, with Spark’s error class
and SQLSTATE.
[`spark_error_details()`](https://pedrobtz.github.io/zuspark/reference/spark_error_details.md)
asks the server for the rest.

``` r

err <- tryCatch(spark_sql(con, "SELECT * FROM no_such_table"), error = identity)
class(err)
#> [1] "zuspark_sql_error"     "zuspark_error"         "zugrpc_error_internal"
#> [4] "zugrpc_error_status"   "zugrpc_error"          "error"                
#> [7] "condition"
err$error_class
#> [1] "TABLE_OR_VIEW_NOT_FOUND"
err$sql_state
#> [1] "42P01"
details <- spark_error_details(err)
names(details[[1]])
#> [1] "type"               "message"            "error_class"       
#> [4] "sql_state"          "message_parameters" "cause"             
#> [7] "stack"              "contexts"
head(details[[1]]$stack, 3)
#>                                                            class
#> 1            org.apache.spark.sql.errors.QueryCompilationErrors$
#> 2 org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt
#> 3           org.apache.spark.sql.catalyst.analysis.CheckAnalysis
#>                              method                         file line
#> 1 tableOrViewNotFoundWithSearchPath QueryCompilationErrors.scala  977
#> 2                     tableNotFound                package.scala   94
#> 3         $anonfun$checkAnalysis0$2          CheckAnalysis.scala  412
```

## Settings, interrupts and disconnecting

[`spark_config()`](https://pedrobtz.github.io/zuspark/reference/spark_config.md)
reads the session’s settings, or sets them by name and returns the
values they had. An R interrupt (Ctrl-C, Esc) while a query runs stops
it on the server;
[`spark_interrupt()`](https://pedrobtz.github.io/zuspark/reference/spark_interrupt.md)
stops every operation of the session.
[`spark_disconnect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)
releases the session.

``` r

old <- spark_config(con, spark.sql.session.timeZone = "UTC")
old
#> spark.sql.session.timeZone 
#>                  "Etc/UTC"
spark_config(con, "spark.sql.session.timeZone")
#> spark.sql.session.timeZone 
#>                      "UTC"
spark_interrupt(con)
spark_disconnect(con)
```

## DBI and dplyr

With the DBI package installed,
[`spark_driver()`](https://pedrobtz.github.io/zuspark/reference/spark_driver.md)
makes a session a DBI connection; with dbplyr too, dplyr pipelines run
on Spark as Spark SQL.
[`dbWriteTable()`](https://dbi.r-dbi.org/reference/dbWriteTable.html)
and [`copy_to()`](https://dplyr.tidyverse.org/reference/copy_to.html)
send small tables as SQL values, and a temporary table is a view of the
session.

``` r

library(dplyr, warn.conflicts = FALSE)
db <- DBI::dbConnect(spark_driver())
sales <- copy_to(db, data.frame(day = rep(1:3, 4), amount = 1:12), "sales", overwrite = TRUE)
summary <- sales |>
  filter(amount > 2) |>
  group_by(day) |>
  summarise(n = n(), total = sum(amount, na.rm = TRUE))
show_query(summary)
#> <SQL>
#> SELECT `day`, COUNT(*) AS `n`, SUM(`amount`) AS `total`
#> FROM `sales`
#> WHERE (`amount` > 2.0)
#> GROUP BY `day`
collect(summary)
#> # A tibble: 3 × 3
#>     day     n total
#>   <int> <dbl> <dbl>
#> 1     1     3    21
#> 2     3     4    30
#> 3     2     3    24
DBI::dbListTables(db)
#> [1] "sales"
DBI::dbDisconnect(db)
```

`dbConnect(spark_driver(), con = con)` wraps a connection you already
have, such as one from
[`spark_connect_databricks()`](https://pedrobtz.github.io/zuspark/reference/spark_connect_databricks.md).

## Next

- [Spark without the
  JVM](https://pedrobtz.github.io/zuspark/articles/spark-without-the-jvm.md):
  what zuspark is made of, and a Spark result handed to DuckDB.
- [Connect to
  Databricks](https://pedrobtz.github.io/zuspark/articles/databricks.md).
- The [reference
  index](https://pedrobtz.github.io/zuspark/reference/index.md) lists
  every function.
