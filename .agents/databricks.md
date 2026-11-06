# zuspark on Databricks

Stage 5's answers to design §7.8 (Q3), checked on 2026-11-06 against a
Databricks Free Edition workspace (serverless compute only, runtime
`19.13.x-aarch64-photon-scala2.13`, Spark `4.2.0`), with zuspark at the
Stage 5 branch over zugrpc's `main`.

## What a workspace needs

| Question | Answer |
| --- | --- |
| Endpoint | `sc://<workspace-host>:443/;use_ssl=true`: gRPC over TLS on 443, on the workspace's own host name. |
| Authentication | A personal access token as `authorization: Bearer …` (zuspark's `token=`). |
| Serverless | An `x-databricks-session-id` header is required. Without it every call fails: `INVALID_ARGUMENT: INVALID_PARAMETER_VALUE: Invalid compute mode headers. Please pass X-Databricks-Cluster-Id for using Spark Connect with classic compute or pass X-Databricks-Session-Id for using Spark Connect with serverless compute.` |
| Its value | Any UUID was accepted, also one different from the request's `session_id`; zuspark sends the session id, which is what makes sense. Header names are case-insensitive (HTTP/2 lower-cases them). |
| Classic compute | `x-databricks-cluster-id` selects a cluster: a made-up id gives `NOT_FOUND: RESOURCE_DOES_NOT_EXIST: No cluster found matching: …`, so the name is right. Not run against a real cluster: Free Edition has none. |
| `user_agent` | Nothing is required: zuspark's default client type and a custom `user_agent` were both accepted. |
| Spark version | `SparkVersion` reports the runtime's Spark (`4.2.0`), which passes the 3.5.0 floor. |

## What works on serverless

Everything v0.1.0 has, with no change for Databricks beyond the header:
connect (Config `get_all` returns only three settings on serverless:
`spark.databricks.execution.timeout`, `spark.sql.ansi.enabled`,
`spark.sql.shuffle.partitions`), `spark_version()`, `spark_sql()`,
`spark_schema()`, `spark_collect()` of 2,000,000 rows, Spark errors as
`zuspark_sql_error` with error class and SQLSTATE, `spark_error_details()`,
`spark_interrupt()` and `spark_disconnect()`.

## Bugs found

- A `session_id` passed as a `zuid::uuid()` object passed the UUID check
  but was never `identical()` to the server's answer, so connecting
  failed with two equal ids in the message. `spark_connect()` now keeps it
  as a plain string.

## The fixture

`tests/testthat/fixtures/databricks/` is one serverless session: connect,
a query's schema and rows, a missing table, disconnect.
`tools/record-databricks.R` records it from `DATABRICKS_HOST` and
`DATABRICKS_TOKEN`. The token and headers are gRPC metadata and never
reach the fixture; the script rewrites any message that names the host,
the workspace, the org id or the user, and fails if one survives. The
2026-11-06 recording held none. The error call has no bytes (the error is
in the gRPC status), so its test raises the error itself.

## Not covered

- A classic cluster, beyond the header name above.
- OAuth (`databricks auth login`) tokens: they are bearer tokens too and
  should work through `token=`, untested.
- Long sessions: the serverless idle timeout and how the server answers a
  session it has dropped.
