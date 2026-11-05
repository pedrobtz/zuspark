# Interrupt operations on the server

`spark_interrupt()` asks the server to stop operations of this session:
every one, or one by its operation id. An R interrupt (Ctrl-C, Esc)
while zuspark waits for a result does this for the running query by
itself, and raises a `zuspark_interrupt_error`.

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
if (FALSE) { # nzchar(Sys.getenv("SPARK_REMOTE"))
con <- spark_connect()
spark_interrupt(con)
spark_disconnect(con)
}
```
