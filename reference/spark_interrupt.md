# Interrupt operations on the server

`spark_interrupt()` asks the server to stop operations of this session:
every one, or one by its operation id. An R interrupt (Ctrl-C, Esc)
while zuspark waits for a query's result does this for the running query
by itself, and raises a `zuspark_interrupt_error`. An interrupt during
any other call, such as
[`spark_schema()`](https://pedrobtz.github.io/zuspark/reference/spark_schema.md)
or
[`spark_config()`](https://pedrobtz.github.io/zuspark/reference/spark_config.md),
cancels that call and raises a `zuspark_interrupt_error` too, without
asking the server to stop anything.

## Usage

``` r
spark_interrupt(con, x = NULL)
```

## Arguments

- con:

  A `zuspark_connection`.

- x:

  `NULL` for every operation of the session, an operation id, or a
  `zuspark_query`, for the last execution of that query.

## Value

The ids of the operations the server interrupted, invisibly.

## Examples

``` r
con <- spark_connect()
spark_interrupt(con)
spark_disconnect(con)
```
