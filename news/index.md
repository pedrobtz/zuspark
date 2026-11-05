# Changelog

## zuspark 0.0.0.9000

- [`spark_connect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)
  opens a session on a Spark Connect server from an `sc://` connection
  string, by default `SPARK_REMOTE`, with PySpark’s parameters: `token`,
  `use_ssl`, `user_id`, `user_agent`, `session_id`, the gRPC message
  size and keepalive, and any other parameter sent as a header. Servers
  older than Spark 3.5.0 are refused.
  [`spark_disconnect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)
  releases the session.

- [`spark_config()`](https://pedrobtz.github.io/zuspark/reference/spark_config.md)
  reads every setting of the session, reads named ones, sets and unsets
  them, returning the previous values so that they can be restored.

- [`spark_version()`](https://pedrobtz.github.io/zuspark/reference/spark_version.md)
  and
  [`spark_session_id()`](https://pedrobtz.github.io/zuspark/reference/spark_version.md)
  report the server’s Spark version and the session’s client and server
  ids.
  [`zuspark_info()`](https://pedrobtz.github.io/zuspark/reference/zuspark_info.md)
  reports the versions zuspark was built with.

- Errors are conditions inheriting from `zuspark_error`:
  `zuspark_url_error` for a malformed connection string,
  `zuspark_version_error` for a server too old, and
  `zuspark_session_error` for a session that was released or that the
  server lost.

- zuspark ships the Spark Connect protocol definitions of Apache Spark
  4.2.1 as a compiled descriptor set. It has no compiled code of its
  own.
