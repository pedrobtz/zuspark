# Spark's full account of an error

A `zuspark_sql_error` carries Spark's error class, SQLSTATE, message
parameters and the exception's class hierarchy. `spark_error_details()`
asks the server for the rest, through `FetchErrorDetails`: every
exception in the cause chain, with its stack trace and the places in the
query it points at. The server keeps these only for a while, and only
when `spark.sql.connect.enrichError.enabled` is on (the default).

## Usage

``` r
spark_error_details(err, con = err$con)
```

## Arguments

- err:

  A `zuspark_sql_error`, as caught with
  [`tryCatch()`](https://rdrr.io/r/base/conditions.html).

- con:

  The connection to ask; by default the one the error came from.

## Value

A list with one element per exception, the root first. Each is a list of
`type` (the class hierarchy), `message`, `error_class`, `sql_state`,
`message_parameters`, `cause` (the index of its cause in the list, or
`NA`), `stack` (a data frame of `class`, `method`, `file` and `line`)
and `contexts` (a data frame of the query fragments).

## Examples

``` r
if (FALSE) { # nzchar(Sys.getenv("SPARK_REMOTE"))
con <- spark_connect()
err <- tryCatch(spark_sql(con, "SELECT * FROM no_such_table"), error = identity)
err$error_class
details <- spark_error_details(err)
details[[1]]$message
spark_disconnect(con)
}
```
