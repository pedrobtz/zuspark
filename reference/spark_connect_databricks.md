# Connect to Databricks

`spark_connect_databricks()` opens a Spark Connect session on a
Databricks workspace: on serverless compute by default, or on a cluster
given by its id. It is
[`spark_connect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)
with the connection string and the header Databricks needs to choose the
compute: for serverless, an `x-databricks-session-id` header carrying
the session id; for a cluster, an `x-databricks-cluster-id` header. The
cluster must be running.

## Usage

``` r
spark_connect_databricks(
  host = Sys.getenv("DATABRICKS_HOST"),
  token = Sys.getenv("DATABRICKS_TOKEN"),
  cluster_id = NULL,
  ...,
  session_id = NULL,
  headers = NULL
)
```

## Arguments

- host:

  The workspace URL, such as
  `"https://dbc-1a2b3c4d-5e6f.cloud.databricks.com"`, with or without
  `https://`. It defaults to the `DATABRICKS_HOST` environment variable.

- token:

  A personal access token. It defaults to the `DATABRICKS_TOKEN`
  environment variable.

- cluster_id:

  `NULL` for serverless compute, or the id of a cluster.

- ...:

  Passed on to
  [`spark_connect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md):
  `user_id`, `user_agent`, `deadline`, or more connection string
  parameters.

- session_id:

  The session's id, a UUID; by default a new version 4 UUID. On
  serverless compute it is also sent as `x-databricks-session-id`.

- headers:

  Extra headers for every call, as a named character vector, for names
  that are not valid in a URL.

## Value

A `zuspark_connection`.

## See also

The [Connect to
Databricks](https://pedrobtz.github.io/zuspark/articles/databricks.html)
article.

## Examples

``` r
if (FALSE) { # nzchar(Sys.getenv("DATABRICKS_TOKEN"))
con <- spark_connect_databricks()
spark_version(con)
spark_disconnect(con)
}
```
