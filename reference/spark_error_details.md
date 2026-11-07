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
and `contexts` (a data frame of the query fragments). When the server no
longer holds the details, `spark_error_details()` raises a
`zuspark_error` instead.

## Examples

``` r
con <- spark_connect()
err <- tryCatch(
  spark_sql(con, "SELECT * FROM no_such_table"),
  error = identity
)
err$error_class
#> [1] "TABLE_OR_VIEW_NOT_FOUND"
details <- spark_error_details(err)
details[[1]]$message
#> [1] "[TABLE_OR_VIEW_NOT_FOUND] The table or view `no_such_table` cannot be found. Verify the spelling and correctness of the schema and catalog.\nSearch path: [`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`].\nIf you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog.\nTo tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01; line 1 pos 14;\n'Project [*]\n+- 'UnresolvedRelation [no_such_table], [], false\n"
spark_disconnect(con)
```
