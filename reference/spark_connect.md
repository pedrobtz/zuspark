# Connect to a Spark Connect server

`spark_connect()` opens a session on a Spark Connect endpoint: it builds
the gRPC channel the connection string describes, proves it with one
`Config` call and one `SparkVersion` call, and refuses servers older
than Spark 3.5.0. `spark_disconnect()` releases the session on the
server and closes the channel.

## Usage

``` r
spark_connect(
  url = Sys.getenv("SPARK_REMOTE"),
  ...,
  user_id = NULL,
  session_id = NULL,
  user_agent = NULL,
  headers = NULL,
  deadline = NULL
)

spark_disconnect(con, allow_reconnect = FALSE)
```

## Arguments

- url:

  A connection string, `sc://host:port/;param=value;...`. It defaults to
  the `SPARK_REMOTE` environment variable. The default port is 15002.
  The reserved parameters are `token` (a bearer token; it turns TLS on,
  except to `localhost` without `use_ssl`), `use_ssl`, `user_id`,
  `user_agent`, `session_id`, `grpc_max_message_size` and
  `grpc_keepalive_enabled`, `_time_ms` and `_timeout_ms`. Every other
  parameter is sent as a header on every call.

- ...:

  Named parameters appended to `url`, such as `token = "..."`.

- user_id, session_id, user_agent:

  The user id sent with every call (by default the `user_id` parameter,
  then `SPARK_USER`, then `USER`), the session's id (a UUID; by default
  a new version 4 UUID), and the text that starts the client type the
  server logs (by default `SPARK_CONNECT_USER_AGENT`, then `"zuspark"`).
  Each takes precedence over the parameter of the same name in `url`.

- headers:

  Extra headers for every call, as a named character vector, for names
  that are not valid in a URL.

- deadline:

  Seconds each call other than a query may take. The default is 30.

- con:

  A `zuspark_connection`.

- allow_reconnect:

  If `TRUE`, the server lets a later connection reuse this session's id.
  By default the id is retired with the session.

## Value

`spark_connect()` returns a `zuspark_connection`. `spark_disconnect()`
returns `TRUE` invisibly, or `FALSE` if the session was already
released.

## Examples

``` r
con <- spark_connect()
con
#> <zuspark_connection> sc://127.0.0.1:15002 (Spark 4.2.1)
#>   session: 9dbdb53c-4f22-4ebd-a41e-91524432a86b
spark_disconnect(con)
```
