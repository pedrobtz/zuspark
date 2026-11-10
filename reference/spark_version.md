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
#> "c518966a-17cd-4aa7-a29b-9c1f56d7bee6" "1fbcc5ea-c733-4efb-b4c3-dd50174b65ac" 
spark_disconnect(con)
```
