# A query's schema

`spark_schema()` asks the server for a query's result schema, through
`AnalyzePlan`, without running the query. Types are Spark SQL's, as DDL
text: `"bigint"`, `"decimal(10,2)"`, `"array<string>"`,
`"struct<a:int,b:string>"`. The Arrow schema the rows arrive in, which
is the server's mapping of these types, is the schema of
[`spark_fetch()`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)'s
stream.

## Usage

``` r
spark_schema(x)
```

## Arguments

- x:

  A `zuspark_query` from
  [`spark_sql()`](https://pedrobtz.github.io/zuspark/reference/spark_sql.md).

## Value

A data frame with one row per column: `name`, `type` and `nullable`.

## Examples

``` r
con <- spark_connect()
spark_schema(spark_sql(con, "SELECT id, CAST(id AS DECIMAL(10, 2)) AS d FROM range(3)"))
#>   name          type nullable
#> 1   id        bigint    FALSE
#> 2    d decimal(10,2)     TRUE
spark_disconnect(con)
```
