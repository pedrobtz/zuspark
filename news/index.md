# Changelog

## zuspark 0.1.0

- [`spark_connect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)
  opens a session on a Spark Connect server from an `sc://` connection
  string, by default `SPARK_REMOTE`, with PySpark’s parameters: `token`,
  `use_ssl`, `user_id`, `user_agent`, `session_id`, the gRPC message
  size and keepalive, and any other parameter sent as a header. Servers
  older than Spark 3.5.0 are refused.
  [`spark_disconnect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)
  releases the session.

- [`spark_sql()`](https://pedrobtz.github.io/zuspark/reference/spark_sql.md)
  runs a statement on the server. A command (DDL, DML, `SET`, `SHOW`)
  runs at once and returns its rows as a data frame; a query returns a
  `zuspark_query`, which
  [`spark_collect()`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)
  turns into a data frame and
  [`spark_fetch()`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)
  into a ‘zuarrow’ Arrow stream, running it afresh each time.
  [`spark_execute()`](https://pedrobtz.github.io/zuspark/reference/spark_sql.md)
  runs a statement for its effect. Results the server cuts into several
  streams are reattached, every row arriving once, and consumed results
  are released as they are read.

- [`spark_driver()`](https://pedrobtz.github.io/zuspark/reference/spark_driver.md)
  makes a Spark Connect session a DBI connection, with
  `DBI::dbConnect(spark_driver(), url)`, so that
  [`dbGetQuery()`](https://dbi.r-dbi.org/reference/dbGetQuery.html),
  [`dbListTables()`](https://dbi.r-dbi.org/reference/dbListTables.html),
  [`dbWriteTable()`](https://dbi.r-dbi.org/reference/dbWriteTable.html)
  and the rest work, and dbplyr translates `dplyr` pipelines to Spark
  SQL with its own Spark backend:
  [`tbl()`](https://dplyr.tidyverse.org/reference/tbl.html),
  [`collect()`](https://dplyr.tidyverse.org/reference/compute.html),
  [`copy_to()`](https://dplyr.tidyverse.org/reference/copy_to.html) and
  [`compute()`](https://dplyr.tidyverse.org/reference/compute.html).
  DBI, dbplyr and dplyr are optional.

- [`spark_schema()`](https://pedrobtz.github.io/zuspark/reference/spark_schema.md)
  gives a query’s schema, as Spark’s types, without running it.

- Results reach R as ‘zuarrow’ converts the Arrow the server sends. A
  `BIGINT` beyond 2^53, a `DECIMAL` or an interval is an error by
  default; `lossy = "character"` in
  [`spark_collect()`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)
  gives its exact text and `lossy = "allow"` a widened value.

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

- A Spark error is a `zuspark_sql_error` carrying Spark’s error class,
  SQLSTATE, message parameters and exception classes;
  [`spark_error_details()`](https://pedrobtz.github.io/zuspark/reference/spark_error_details.md)
  fetches its full cause chain and stack trace. Other failed calls are
  `zuspark_rpc_error`.

- [`spark_connect_databricks()`](https://pedrobtz.github.io/zuspark/reference/spark_connect_databricks.md)
  connects to a Databricks workspace, on serverless compute by default
  or on a cluster given by `cluster_id`, from `DATABRICKS_HOST` and
  `DATABRICKS_TOKEN`. The “Connect to Databricks” article shows how,
  including Databricks Free Edition.

- [`spark_connect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)
  accepts a `session_id` with attributes, such as a
  [`zuid::uuid()`](https://pedrobtz.github.io/zuid/reference/uuid.html),
  instead of failing every session check.

- Arrow data from the server that zuarrow refuses as malformed, over its
  limits or that does not decompress is a `zuspark_protocol_error`;
  before, only malformed data was.

- A connection, query or Spark error saved with
  [`saveRDS()`](https://rdrr.io/r/base/readRDS.html) or sent to another
  process (callr, future, mirai, targets) no longer carries the bearer
  token: the gRPC channel stays in the R session that opened it (#26).

- A statement whose result is complete is no longer reported as failed
  when the release that follows it fails; a failed release could invite
  running an `INSERT` twice (#27).

- [`spark_config()`](https://pedrobtz.github.io/zuspark/reference/spark_config.md)
  and
  [`spark_connect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)’s
  `...` send numbers in full: `100000` was sent as `1e+05`, which Spark
  refuses. `NA` and non-finite values are refused (#28).

- [`spark_error_details()`](https://pedrobtz.github.io/zuspark/reference/spark_error_details.md)
  raises an error when the server no longer holds the details, instead
  of returning a blank entry (#29).

- Every invalid argument to
  [`spark_connect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)
  is a `zuspark_url_error` naming the part: `user_id`, `user_agent`,
  `deadline`, header names and values, and keepalive and message sizes
  (#30).

- A connection-string value with an invalid percent-escape, and an empty
  `token=`, are refused instead of being silently changed (#31).

- An R interrupt during any call is a `zuspark_interrupt_error`; only an
  interrupt while a query’s result is read also stops the job on the
  server. When that Interrupt call itself fails, the error says so
  instead of reporting the operation as finished (#33, \#55).

- [`spark_interrupt()`](https://pedrobtz.github.io/zuspark/reference/spark_interrupt.md)
  refuses a query of another connection (#35).

- A printed connection or query hides every parameter sent as a header,
  and any user name and password before the host, not only `token`
  (#36).

- A connect that fails after the session was made, or refuses a server
  older than Spark 3.5, releases that session on the server (#37).

- An abandoned read cancels its stream, a failed release during a read
  no longer fails a query, and a cut before a reattach’s first response
  is reattached again (#32, \#34).

- [`spark_disconnect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)
  refuses an `allow_reconnect` that is not `TRUE` or `FALSE` (#38).

- A malformed connection string’s error never prints a token: the
  missing-host message redacts it, and a malformed parameter is named by
  its key only (#25).

- An R interrupt while a query runs cancels it and stops the job on the
  server (`zuspark_interrupt_error`);
  [`spark_interrupt()`](https://pedrobtz.github.io/zuspark/reference/spark_interrupt.md)
  stops every operation of the session, or one.

- Errors are conditions inheriting from `zuspark_error`:
  `zuspark_url_error` for a malformed connection string,
  `zuspark_version_error` for a server too old, and
  `zuspark_session_error` for a session that was released or that the
  server lost, and `zuspark_protocol_error` for a response that
  contradicts the protocol.

- zuspark ships the Spark Connect protocol definitions of Apache Spark
  4.2.1 as a compiled descriptor set. It has no compiled code of its
  own.
