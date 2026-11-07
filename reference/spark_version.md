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
#> "da4466b6-e3d7-41fe-826b-a7718f58124f" "4df26a28-8a2e-417b-a1bb-1e603f7ddabd" 
spark_disconnect(con)
```
