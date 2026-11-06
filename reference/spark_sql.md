# Run SQL on the server

`spark_sql()` sends a statement to the server. A command (DDL, DML,
`SET`, `SHOW`, a SQL script) runs at once, and its rows come back as a
data frame. A query (`SELECT`, `WITH`, `VALUES`, ...) comes back as a
`zuspark_query`, which holds the server's plan for it and no rows; it
runs when
[`spark_collect()`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)
or
[`spark_fetch()`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)
asks for them, and runs again on every such call. `spark_execute()` runs
a statement for its effect and discards its rows.

## Usage

``` r
spark_sql(con, sql)

spark_execute(con, sql)
```

## Arguments

- con:

  A `zuspark_connection`.

- sql:

  A Spark SQL statement, as a single string.

## Value

`spark_sql()` returns a `zuspark_query` for a query, or a data frame for
a command. `spark_execute()` returns the number of rows the statement
produced, invisibly.

## Examples

``` r
con <- spark_connect()
q <- spark_sql(con, "SELECT id, id * 2 AS twice FROM range(5)")
q
#> <zuspark_query> SELECT id, id * 2 AS twice FROM range(5)
#>   on sc://127.0.0.1:15002 (not run yet)
spark_collect(q)
#>   id twice
#> 1  0     0
#> 2  1     2
#> 3  2     4
#> 4  3     6
#> 5  4     8
spark_sql(con, "SHOW DATABASES")
#>   namespace
#> 1   default
spark_disconnect(con)
```
