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

`spark_connect()` takes an `sc://` connection string, by default the one
in the `SPARK_REMOTE` environment variable. A local Spark Connect server
listens on port 15002.

``` r

con <- spark_connect("sc://localhost:15002")
spark_version(con)
```

Parameters go after a `;`. `token` sends a bearer token and turns TLS
on; any parameter zuspark doesn’t recognise is sent as a request header.

``` r

con <- spark_connect("sc://spark.example.com:443/;token=my-token")
```

## Run SQL

`spark_sql()` sends a statement. A query comes back as a
`zuspark_query`, which holds the plan but no rows yet; nothing is read
until you ask for it.

``` r

q <- spark_sql(con, "SELECT id, id * 2 AS twice FROM range(10)")
spark_schema(q)
```

Commands such as DDL, DML and `SET` run at once, and `spark_sql()`
returns the rows they produce as a data frame.

``` r

spark_sql(con, "CREATE TEMPORARY VIEW small AS SELECT * FROM range(5)")
```

## Collect the result

`spark_collect()` runs the query and returns one data frame. Types that
R can’t hold exactly, such as a `BIGINT` beyond 2^53, are an error by
default; `lossy` chooses text or a widened type instead.

``` r

# Open: whether spark_collect() returns a plain data frame or a zutable
# (the design proposes plain, unless a consumer asks).
spark_collect(q)
spark_collect(q, lossy = "character")
```

`spark_fetch()` returns the result as an Arrow stream instead, for code
that consumes Arrow directly.

``` r

stream <- spark_fetch(q)
```

## Settings, interrupts and disconnecting

`spark_config()` reads the session’s settings, or sets them by name.
`spark_interrupt()` stops a running query on the server, or every
operation when given no query. `spark_disconnect()` releases the
session.

``` r

spark_config(con, spark.sql.session.timeZone = "UTC")
spark_interrupt(con)
spark_disconnect(con)
```

## Next

The [reference
index](https://pedrobtz.github.io/zuspark/reference/index.md) lists
every function. An article on running Spark without the JVM, including
handing results to DuckDB, is planned.
