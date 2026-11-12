# Inspect a session

`spark_version()` is the Spark version the server reported when the
session was opened. `spark_session_id()` gives the session's two ids:
the client's, sent with every call, and the server's, which the server
returns on every response and which changes if the server restarts.

## Usage

``` r
spark_version(con)

spark_session_id(con)
```

## Arguments

- con:

  A `zuspark_connection`.

## Value

`spark_version()` returns a string such as `"4.2.1"`.
`spark_session_id()` returns a character vector with names `client` and
`server`.

## Examples

``` r
con <- spark_connect()
spark_version(con)
#> [1] "4.2.1"
spark_session_id(con)
#>                                 client                                 server 
#> "81d2d887-6c31-47a1-add8-58bfd2495722" "2bc98296-fba7-44a8-a72c-91aca9e78992" 
spark_disconnect(con)
```
