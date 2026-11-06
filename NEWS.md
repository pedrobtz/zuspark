# zuspark 0.0.0.9000

* `spark_connect()` opens a session on a Spark Connect server from an
  `sc://` connection string, by default `SPARK_REMOTE`, with PySpark's
  parameters: `token`, `use_ssl`, `user_id`, `user_agent`, `session_id`,
  the gRPC message size and keepalive, and any other parameter sent as a
  header. Servers older than Spark 3.5.0 are refused.
  `spark_disconnect()` releases the session.

* `spark_sql()` runs a statement on the server. A command (DDL, DML,
  `SET`, `SHOW`) runs at once and returns its rows as a data frame; a
  query returns a `zuspark_query`, which `spark_collect()` turns into a
  data frame and `spark_fetch()` into a 'zuarrow' Arrow stream, running
  it afresh each time. `spark_execute()` runs a statement for its effect.
  Results the server cuts into several streams are reattached, every row
  arriving once, and consumed results are released as they are read.

* `spark_schema()` gives a query's schema, as Spark's types, without
  running it.

* Results reach R as 'zuarrow' converts the Arrow the server sends. A
  `BIGINT` beyond 2^53, a `DECIMAL` or an interval is an error by
  default; `lossy = "character"` in `spark_collect()` gives its
  exact text and `lossy = "allow"` a widened value.

* `spark_config()` reads every setting of the session, reads named ones,
  sets and unsets them, returning the previous values so that they can be
  restored.

* `spark_version()` and `spark_session_id()` report the server's Spark
  version and the session's client and server ids. `zuspark_info()`
  reports the versions zuspark was built with.

* A Spark error is a `zuspark_sql_error` carrying Spark's error class,
  SQLSTATE, message parameters and exception classes;
  `spark_error_details()` fetches its full cause chain and stack trace.
  Other failed calls are `zuspark_rpc_error`.

* `spark_connect_databricks()` connects to a Databricks workspace, on
  serverless compute by default or on a cluster given by `cluster_id`,
  from `DATABRICKS_HOST` and `DATABRICKS_TOKEN`. The "Connect to
  Databricks" article shows how, including Databricks Free Edition.

* `spark_connect()` accepts a `session_id` with attributes, such as a
  `zuid::uuid()`, instead of failing every session check.

* Arrow data from the server that zuarrow refuses as malformed, over its
  limits or not decompressible is a `zuspark_protocol_error`; before,
  only malformed data was.

* An R interrupt while a query runs cancels it and stops the job on the
  server (`zuspark_interrupt_error`); `spark_interrupt()` stops every
  operation of the session, or one.

* Errors are conditions inheriting from `zuspark_error`:
  `zuspark_url_error` for a malformed connection string,
  `zuspark_version_error` for a server too old, and
  `zuspark_session_error` for a session that was released or that the
  server lost, and `zuspark_protocol_error` for a response that
  contradicts the protocol.

* zuspark ships the Spark Connect protocol definitions of Apache Spark
  4.2.1 as a compiled descriptor set. It has no compiled code of its own.
