# Connect to Databricks

Databricks workspaces speak Spark Connect, so zuspark can query them
directly: no `databricks-connect`, no Python and no JVM. You need the
workspace’s URL, a personal access token, and one extra header that
tells Databricks which compute to run on. This article shows serverless
compute, which is all that Databricks Free Edition offers and which
needs no cluster to be started.

``` r

library(zuspark)
```

## Get a workspace URL and a token

The workspace URL is the start of the address in your browser while you
are in the workspace, up to the domain, such as
`https://dbc-1a2b3c4d-5e6f.cloud.databricks.com`. Leave out the path and
the `?o=` part.

To create a token, click your avatar in the top right corner, then
**Settings**, **Developer**, and **Manage** next to **Access tokens**.
Generate a new token and copy it: Databricks shows it only once.

Keep both out of your scripts. One way is to put them in `~/.Renviron`,
which R reads at start-up:

``` sh
DATABRICKS_HOST=https://dbc-1a2b3c4d-5e6f.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
```

## Connect to serverless compute

[`spark_connect_databricks()`](https://pedrobtz.github.io/zuspark/reference/spark_connect_databricks.md)
reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN`, and connects to the
workspace’s serverless compute:

``` r

con <- spark_connect_databricks()
spark_version(con)
```

[`spark_version()`](https://pedrobtz.github.io/zuspark/reference/spark_version.md)
reports the Spark version of the Databricks runtime behind the session.
Serverless compute starts by itself when the session connects, and the
first query can take a few seconds while it does.

What it does is plain Spark Connect: TLS on port 443, the token as a
bearer token, and an `x-databricks-session-id` header carrying the
session id. Without that header, the server answers *“Invalid compute
mode headers”*. The same connection with
[`spark_connect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md):

``` r

host <- sub("^https://", "", Sys.getenv("DATABRICKS_HOST"))
session <- as.character(zuid::uuid(version = 4))

con <- spark_connect(
  sprintf("sc://%s:443/;use_ssl=true", host),
  token = Sys.getenv("DATABRICKS_TOKEN"),
  session_id = session,
  headers = c("x-databricks-session-id" = session)
)
```

## Run SQL

From here on, a Databricks session works like any other Spark Connect
session:

``` r

q <- spark_sql(con, "SELECT id, id * 2 AS twice FROM range(5)")
spark_schema(q)
spark_collect(q)
```

Tables in Unity Catalog are queried by their three-part names, such as
those of the `samples` catalog, where a workspace has it:

``` r

spark_collect(spark_sql(con, "
  SELECT pickup_zip, count(*) AS trips
  FROM samples.nyctaxi.trips
  GROUP BY pickup_zip
  ORDER BY trips DESC
  LIMIT 5
"))
```

Spark’s errors arrive with their error class and SQLSTATE, and
[`spark_error_details()`](https://pedrobtz.github.io/zuspark/reference/spark_error_details.md)
asks the workspace for the rest:

``` r

err <- tryCatch(spark_sql(con, "SELECT * FROM no_such_table"), error = identity)
err$error_class
err$sql_state
spark_error_details(err)[[1]]$message
```

## Disconnect

[`spark_disconnect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)
releases the session on the workspace, so its serverless compute can
stop:

``` r

spark_disconnect(con)
```

## Classic compute

On a workspace with clusters (not Free Edition), give the cluster’s id.
zuspark then sends it in an `x-databricks-cluster-id` header instead of
the session header. The cluster must be running. This path has not been
run against a real cluster yet:

``` r

con <- spark_connect_databricks(cluster_id = "0123-456789-abcdefgh")
```
