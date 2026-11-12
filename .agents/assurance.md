# Assurance

How this package meets `.claude/shared/assurance.md`. CI checks this
file (r-actions `assurance.yml`), so keep the headings and table
columns as they are.

- **Statuses:** a cell is a fact, `gap #<issue>` or `n/a: <reason>`.
- **Paths:** test files are under `tests/testthat/`.
- **Fixtures:** the requests and responses under `fixtures/` were
  recorded by `tools/record-fixtures.R` from zuspark talking to Spark
  Connect 4.2.1, and `fixtures/types/` holds PySpark's Arrow bytes for
  the same queries, recorded by `tools/pyspark-types.py`. A replayed
  response is therefore Spark's answer, not one built by hand. A replayed
  request is zuspark's own earlier request, so on its own it guards
  against regressions; Spark answering it is the oracle.
- **Live:** `test-live-*.R` run only in `live.yaml`, against Spark
  Connect 4.2.1 and a second 4.2.1 server that cuts its streams after 1 s
  or 1 MiB.
- **Pure R:** there is no `src/` (D3), so `compiled` does not apply, and
  r-actions' `fuzz.yml`, which is libFuzzer over C, cannot run here. The
  fuzzing gap needs a workflow of zuspark's own, as zugrpc's `fuzz.yaml`
  is.

## Kinds

`untrusted-input` `spec` `lifecycle` `emits`

## Required checks

`Where` names the workflow file under `.github/workflows/` that runs the
check. Add `(own)` when that workflow is the package's own rather than a
call to r-actions.

| Check      | Status    | Where |
| ---------- | --------- | ----- |
| fuzz       | gap #105 |       |
| fuzz-reach | gap #106 |       |

## Boundaries

One row per boundary the package crosses. An oracle knows the answer:

- a conformance suite or test vectors;
- a reference implementation;
- a round trip;
- equivalent inputs compared;
- the bytes the other side received.

"No crash" is not an oracle.

<!-- markdownlint-disable MD013 -->
| Boundary | Oracle | Carried by |
| -------- | ------ | ---------- |
| ExecutePlan response stream, decoding (batches, SqlCommandResult, schema, metrics, ResultComplete) | responses recorded from Spark 4.2.1, replayed, give known rows (`range(5)` and its columns); live, the same queries and `range(10^5)` in many batches | test-sql.R, test-execute.R, test-live-sql.R, test-live-protocol.R |
| ExecutePlan response stream, damaged | a recorded response cut before its last byte, a 2^31-byte length prefix, a 2^52 `row_count`, a 2^40 `start_offset`, and a 2^31 - 1 Arrow metadata length are each asserted to be a `zuspark_protocol_error`; sampled cuts and flips must end in a result or a `zuspark_error` | test-hostile.R, test-execute.R |
| Unary responses, decoding (Config, AnalyzePlan, ReleaseExecute, ReleaseSession, Interrupt) | answers recorded from Spark 4.2.1, replayed; the settings read back equal those set; live, the same calls | test-session.R, test-interrupt.R, test-live-session.R |
| Reattachable execution (ReattachExecute, ReleaseExecute) | streams recorded from a server cutting at 2 KB, replayed: every row once; the release rule, run over the recorded responses, gives the `release_until` ids sent; live, `range(10^8)` through the cutting server gives 10^8 rows, no duplicate, the known sum | test-execute.R, test-live-sql.R |
| Session ids on every response | each response path is given an answer for another session and must raise `zuspark_session_error`; the server-side id is sent back once learnt; live, a released id is refused by Spark itself | test-session.R, test-live-session.R |
| Requests, encoding (every RPC zuspark sends) | Spark 4.2.1 accepts and answers each one, recorded and live; replayed requests equal the recording field by field (regression only) | test-session.R, test-sql.R, test-live-*.R |
| Requests, against PySpark's | PySpark's requests for the same scenarios, compared field by field less the ids | gap #107 |
| SqlCommand plan, encoding | both `input` and the deprecated `sql` carry the statement (Q2); the encoded bytes are snapshotted; Spark 4.2.1 runs it | test-sql.R, \_snaps/sql.md, test-live-sql.R |
| ArrowBatch payloads to R | PySpark's Arrow bytes for 21 Spark types convert to known R values under each `lossy` policy; live, the Arrow schema and values zuspark receives equal PySpark's for the same queries; `row_count` and `start_offset` are checked against the batches | test-types.R, test-live-types.R, test-execute.R |
| DataType to DDL text (`spark_schema()`) | the expected text is written by hand in `types.csv` and the unit cases, and checked live against Spark's DataType for 21 types; nothing compares it with Spark's own text for a type | gap #108 |
| Spark error (ErrorInfo in `grpc-status-details-bin`) to `zuspark_sql_error` | errors recorded from Spark 4.2.1, replayed; live, a typo and a missing table give Spark's error class, SQLSTATE and message parameters | test-errors.R, test-live-errors.R |
| JSON strings in ErrorInfo metadata (`classes`, `messageParameters`) | hand-written cases: escapes, surrogate pairs, cut-short escapes; nothing compares the hand-written parser with a JSON library | gap #109 |
| FetchErrorDetails response to the cause chain | an answer recorded from Spark 4.2.1, replayed; live, a missing table's details | test-errors.R, test-live-errors.R |
| `sc://` connection string, parsing | PySpark's four INVALID_CONNECT_URL messages word for word; defaults, parameter rules and TLS choice hand-worked from DefaultChannelBuilder at v4.2.1; nothing runs PySpark's parser on the same strings | gap #110 |
| Request metadata (bearer token, string parameters as headers, Databricks compute headers), encoding | the headers given to the transport are asserted exactly; a Databricks Free Edition workspace accepted them (the `databricks` fixture, replayed) | test-session.R, test-databricks.R |
| DBI writes: Spark SQL quoting and literals, emitted | round trips through live Spark: a string with quotes and backslashes, an identifier with backticks, a data frame through a temporary table, doubles at their edges, an instant in a non-UTC session | test-dbi.R, test-live-dbi.R |
| DBI interface | DBItest was run once before #89 merged; it is not in the suite | gap #111 |
| dbplyr translation | dbplyr's own Spark SQL backend, registered for the connection; live, a pipeline's result | test-dbi.R, test-live-dbi.R |
| Bundled descriptor set | SHA-256 recorded by `tools/build-descriptors.sh` | test-proto.R |
<!-- markdownlint-enable MD013 -->

## Objects

One row per object that outlives the call that made it. "Sequences" is
the test of its out-of-order use (abort then read, use after end, use
after free). The guard columns name the test, or say why the guard
cannot apply.

<!-- markdownlint-disable MD013 -->
| Object | Sequences | Fork | Unwind | Owner |
| ------ | --------- | ---- | ------ | ----- |
| `zuspark_connection` (an environment; its zugrpc channel in the namespace's `channels` registry; the server's session) | test-session.R, test-live-protocol.R: every verb after disconnect, disconnect twice, a failed connect, a connect failing after Config, a copy unserialised in another session, a saved connection without the token | gap #112 | test-session.R: a failure while connecting releases the session and closes the channel | gap #113 |
| ExecutePlan operation (the server's buffered result and the open stream) | test-execute.R, test-interrupt.R: abandoned, cut, no progress, failed reattach, failed release after completion, interrupt | n/a: lives inside one `execute_plan()` call and is never returned to R | test-execute.R: on exit the open stream is cancelled and everything is released | test-execute.R, test-sql.R: `execute_plan()` releases all once, on completion or abandonment |
| `zuspark_query` (an environment holding the connection and the plan) | test-sql.R, test-live-sql.R, test-live-protocol.R, test-interrupt.R: run again each time, collect after disconnect, interrupt from another connection | n/a: holds no channel; it reaches one only through its connection (row above) | n/a: holds no native state, and its run fields are set after the execution | n/a: holds no resource |
| `spark_fetch()` stream (zuarrow's handle over batches already received) | gap #114 | n/a: zuarrow's handle over R raw vectors; zuarrow guards it | n/a: zuarrow's | n/a: zuarrow owns it; the execution is released before it is returned |
| DBI `ZusparkResult` and `ZusparkConnection` | test-dbi.R: a cleared result refuses use, clearing twice warns; a stale result, `dbDisconnect()` twice and use after disconnect are DBItest's cases, not run | n/a: wrap the objects above and hold R data only | n/a: R data only | n/a: the connection row's owner |
| Spark Connect descriptor pool, cached for the session | n/a: built once from the bundled file and only read after | n/a: read-only, so a child's copy is the parent's | n/a: assigned only after zuproto returns it | n/a: the namespace holds it for the session |
<!-- markdownlint-enable MD013 -->

## Limits

One row per limit the package enforces. "Straddled by" is the test of
`limit - 1`, `limit` and `limit + 1` on every path that reaches it.

<!-- markdownlint-disable MD013 -->
| Limit | Unit | Straddled by |
| ----- | ---- | ------------ |
| inline release every 16 responses (`release_every`) | responses | gap #115 |
| no progress across three reattaches | consecutive reattaches without a new response | gap #116 |
| `user_agent` at most 2048 characters once percent-encoded | encoded characters | gap #117 |
| server at least Spark 3.5.0 | version | gap #118 |
| port 0 to 65535 | port number | gap #119 |
| `deadline` above 0, default 30 | seconds | gap #120 |
| `grpc_max_message_size`, positive, passed on as zugrpc's `max_message` (0 to 4294967295) | bytes | gap #121, after bug #103 |
| `grpc_keepalive_time_ms` and `grpc_keepalive_timeout_ms`, positive, passed to `zugrpc::grpc_keepalive()` (1 ms to 2147483.647 s) | milliseconds | gap #121, after bug #103 |
| `zuspark.write_warn_rows`, default 10000 | rows written as SQL literals | gap #122 |
| zuarrow's `arrow_limits()` on each ArrowBatch | zuarrow's units (bytes, depth, count) | zuarrow's own suite straddles them; test-hostile.R checks that one over the limit becomes a `zuspark_protocol_error` |
<!-- markdownlint-enable MD013 -->
