# Read and change the session's configuration

One function for the session's Spark SQL configuration, through Spark
Connect's `Config` call. With no arguments it returns every setting.
Unnamed strings name settings to read. Named arguments set settings, and
a named `NULL` or `NA` unsets one, returning to the server's default.
The server's warnings, such as for a deprecated setting, become R
warnings.

## Usage

``` r
spark_config(con, ...)
```

## Arguments

- con:

  A `zuspark_connection`.

- ...:

  Setting names to read, or `name = value` pairs to set. A logical value
  is sent as `"true"` or `"false"`.

## Value

Reading, a named character vector of values, `NA` for a setting that has
none. Setting, the previous values invisibly, in the same form, so that
passing them back restores the session:
`old <- spark_config(con, a = "1")`, then
`do.call(spark_config, c(list(con), as.list(old)))`.

## Examples

``` r
con <- spark_connect()
spark_config(con, "spark.sql.session.timeZone")
#> spark.sql.session.timeZone 
#>                  "Etc/UTC" 
old <- spark_config(con, spark.sql.session.timeZone = "UTC")
do.call(spark_config, c(list(con), as.list(old)))
spark_disconnect(con)
```
