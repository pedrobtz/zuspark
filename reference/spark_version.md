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
#> "ff01a589-7b6f-4e4b-a18c-2c5fae68950a" "0c7ad18a-2b43-4999-98ec-82e93fcb3fa5" 
spark_disconnect(con)
```
