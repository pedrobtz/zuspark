# RFC 0070: `zuspark`, a native Spark Connect client with no JVM and no Python

Adopted from [RFC 0070](https://github.com/pedrobtz/packages/blob/4af0498c103443c19742e2a8cdd992137c3fa8e3/rfcs/not-started/0070-zuspark-spark-connect-client.md)
on 2026-11-05. This file is the single source of truth for the design; the
RFC is now a pointer here.

- **Status:** Proposed. Merging this file adopts the design as the starting
  point for the package's own `.agents/design.md`; each decision in section
  13 stays the maintainer's until the package repository records it.
  Release is gated on five siblings reaching CRAN (section 4): the four
  `Imports` and, beneath `zugrpc`, a `zuhttp` release with HTTP/2, which
  is planned after its 1.0. `zugrpc` has no repository yet.
- **Date:** 2026-11-03.
- **Packages:** `zuspark` (not yet created; crandb returns `not_found` and
  there is no `pedrobtz/zuspark` repository, both checked on 2026-11-03).
  `Imports: zugrpc, zuproto (>= 0.1.0), zuarrow, zuid (>= 0.1.0)`.
  `Suggests: testthat (>= 3.0.0), withr, zuint, decimal, zutempo, duckdb,
  nanoarrow, zujson`. No `LinkingTo`, no `src/` (D3). Ships one
  `FileDescriptorSet` compiled from Spark's `.proto` files at a pinned tag
  (Apache-2.0, with its `NOTICE`), plus `google/rpc/status.proto` and
  `error_details.proto`. MIT for `zuspark`'s own code.
- **Source:** the request of 2026-11-03 for a Spark Connect client in R
  without a JVM or Python, and the design agreed with the maintainer that
  day, amended the same day to put HTTP/2 in `zuhttp`; `apache/spark`
  at `v4.3.0` (protos, the PySpark client, the server, the docs) and at
  `v3.4.0`, `v3.5.0` and `v4.0.0` for the protocol's history; Docker
  Hub; the design documents and stage issues of `zuproto`, `zuarrow`,
  `zuid` and `zuhttp`; `rfcs/more-candidates.md` §2 and §3; crandb.
  Section 17 lists each. Nothing has been built.

## 1. What zuspark is

`zuspark` connects R to a Spark Connect endpoint, runs SQL there, and
brings the result back as Arrow. It speaks the protocol itself: gRPC
through `zugrpc`, which is pure R over `zuhttp`'s HTTP/2, protobuf plans
through `zuproto`, Arrow IPC result batches through `zuarrow`. It needs
no JVM, no Python, no Spark installation, no `spark_install()`, no
gateway process and no system library beyond the TLS `zuhttp` already
uses.

That is the minimal functionality PySpark offers over Spark Connect, cut
down to SQL: connect, run a statement, read a schema, collect rows,
interrupt, disconnect.

> **Speak Spark Connect's SQL subset exactly, hand every row to
> `zuarrow` untouched, and refuse what it cannot represent.**

### 1.1 What zuspark is not

- **Not a dplyr backend and not a DBI driver.** v0.1.0 imports neither.
  The API is DBI-shaped (`spark_connect()`, `spark_sql()`,
  `spark_collect()`, `spark_disconnect()`) so that a DBI shim, and
  `dbplyr` through its Spark SQL backend (`backend-spark-sql.R`, dbplyr
  2.6.0, CRAN 2026-06-17), are later versions (§14), not missing pieces.
- **Not a plan builder.** No relation or expression constructors. One
  relation type is sent, `SQL`, inside one command, `SqlCommand` (§7.3).
  The rest of `relations.proto` and `expressions.proto` rides in the
  descriptor set and is never built.
- **Not a UDF runner.** Spark Connect has Python and Scala user-defined
  functions and no R ones; there is no `spark_apply()`.
- **Not ML, not streaming, not pipelines.** `ml.proto`, the
  `StreamingQuery*` commands and `pipelines.proto` are out of scope.
- **Not a local Spark.** No local mode, no cluster launcher. The family's
  answer at laptop scale is DuckDB, `qio` for Parquet and `zulake`
  (`more-candidates.md` §3) for Delta tables; `zuspark` is for data that
  already lives behind a Spark Connect endpoint.

### 1.2 Where it sits among existing CRAN packages

Checked on crandb on 2026-11-03.

- **`sparklyr`** 1.9.6 (Apache-2.0, 2026-10-05, 20 imports): a JVM, a
  Scala JAR, a gateway socket, `invoke()`, `dbplyr` translation, ML
  wrappers, `spark_apply()`. For Spark Connect and Databricks Connect it
  delegates to `pysparklyr`.
- **`pysparklyr`** 0.2.2 (MIT, 2026-07-16, 22 imports including `arrow`,
  `httr2`, `processx`, `DBI`, `dbplyr`, `reticulate` and `connectcreds`)
  drives PySpark through `reticulate`, so a Python environment with
  `pyspark` or `databricks-connect` comes first. Its Databricks path
  calls `DatabricksSession.builder.remote(host, token, cluster_id)` in
  Python (`R/connect-databricks*.R`).
- **`SparkR`** was archived from CRAN on 2021-06-28. It still ships
  inside Spark (`R/pkg/DESCRIPTION` is `4.3.0` at `v4.3.0`), is
  "deprecated from Apache Spark 4.0.0 and will be removed in a future
  version" per the 4.0.0 documentation, and talks to a local JVM, not to
  Spark Connect. The design brief said SparkR was removed in 4.0; it was
  not.
- **`sparkconnect`** and **`sparkr`** are `not_found`. No CRAN package
  speaks Spark Connect natively, and none has an HTTP/2 client to build
  one on: `curl` exposes libcurl's HTTP/2 for requests, not the
  trailer-bearing streams gRPC needs.

Reference clients outside R: PySpark's own (the one this RFC reads), the
Scala client in Spark, `apache/spark-connect-go` (created 2023-05-30,
250 stars) and `apache/spark-connect-rust` (created 2025-05-26, pushed
2026-10-31, 48 stars, "Rust core + Python wrapper"). The family does C
and R, so none is vendored; all are read as specifications.

What is left for `zuspark`, as narrowly as it is true: **Spark SQL from
R with nothing but R packages installed**, which no CRAN package offers;
results that go to `duckdb` or `nanoarrow` without a copy (§7.6); and
exact types at the R edge (§7.7).

### 1.3 The admission test, applied honestly

`base-layer-candidates.md` §2 asks four things.

1. **A well-specified protocol with a reference.** Passes. The `.proto`
   files are the specification; PySpark is the reference client and the
   conformance oracle (§9).
2. **Vendorable permissive code or a small specification.** Passes. The
   protocol files are Apache-2.0 and about 169 KB of text at `v4.3.0`;
   `zuspark` compiles them once and is pure R over four siblings, of
   which only `zuhttp` and `zuarrow` carry C (D3).
3. **Two siblings that would call it.** Fails. `zulake` could read a
   Delta log through a Spark endpoint and `quak` could query Databricks;
   neither has asked.
4. **A real weakness in base R.** Passes. Base R has no HTTP/2, no gRPC
   and no Arrow; CRAN reaches Spark only through a JVM or Python.

So `zuspark` is an application package, like `zunlp`, admitted on point 4
and on the maintainer's request (D1).

## 2. Scope

**In v0.1.0:**

- The `sc://` connection string of §7.1, TLS and bearer tokens through
  `zugrpc` and `zuhttp`, arbitrary headers from unreserved parameters.
- Session lifecycle: client UUID from `zuid`, `user_context`,
  `client_type`, the server-side session id echo, `ReleaseSession`.
- `Config`: get, get-all, set, unset, through one function.
- SQL as `SqlCommand` over a `SQL` relation, the two-step protocol of
  §7.3, the eager path for DDL, DML and scripts.
- Reattachable execution: `operation_id`, `ReattachExecute`,
  `ReleaseExecute`, the server's stream cut-over (§7.4).
- Result batches through `zuarrow`'s IPC reader, lazily or all at once,
  with `zuarrow`'s `lossy` policy at the R edge (§7.5, §7.7).
- `AnalyzePlan.Schema` and `AnalyzePlan.SparkVersion`.
- Errors from `grpc-status-details-bin`, `FetchErrorDetails` on request,
  interrupts and deadlines (§8).
- Databricks: whatever §7.8 can verify in Stage 5.

**Not in v0.1.0** (§14): DBI, dbplyr, a plan builder, parameterised SQL,
`AddArtifacts`, UDFs, ML, streaming, pipelines, `CloneSession`, result
chunking, OAuth or any token acquisition, retries beyond reattach, a C
API.

## 3. Position in the `zu*` family

| | zuspark |
| --- | --- |
| Role | application (client) |
| R prefix | `spark_` |
| Info function | `zuspark_info()` |
| Root condition class | `zuspark_error` |
| Public C prefix | none; no `src/` (D3) |
| From C | no |
| Hides symbols | not applicable |
| Vendored code | none; one compiled descriptor set in `inst/` |
| `Depends: R` | 4.1 (R2) |
| Language | en-GB (R2) |

`spark_` is `sparklyr`'s prefix too, accepted so that a user moving over
finds the same verbs; the packages share no session class, so no S3
method collides (D11). No RFC here uses the prefix (searched
2026-11-03).

**Relationships:**

- **`zugrpc`** (RFC 0069, written alongside this one): the gRPC layer,
  pure R over `zuhttp`'s request API: five-byte message framing,
  `content-type: application/grpc`, request metadata, `grpc-timeout`,
  and the trailing `grpc-status` and `grpc-status-details-bin`.
  `zuspark` needs a channel with a bearer credential, unary calls, one
  server-streaming call whose body arrives as a callback and can be
  cancelled from an R interrupt, and deadlines. Message bytes are opaque
  to `zugrpc`; encoding is `zuspark`'s through `zuproto`.

  *As built, 2026-11-05 (zugrpc `main` `ba9d83b`, reported by the zugrpc
  session):* `grpc_channel(url, headers, tls, keepalive =
  grpc_keepalive(60, 20), max_message = 128 MiB, client = NULL)`;
  `grpc_call()` returns a list of `message` (raw), `headers`, `trailers`
  (named lists, `-bin` values raw) and `status`; `grpc_stream()` returns
  an object with `$headers()`, `$next_message()` (raw, `NULL` at the
  end), `$trailers()`, `$status()` and `$cancel()`; `grpc_close()` and
  `zugrpc_info()`. The stream is pulled one message at a time from R, not
  delivered by callback, which suits §7.5's collect-then-decode. No
  client-streaming or bidirectional calls (zugrpc D5), so `AddArtifacts`
  (§14's `spark_write()`) needs zugrpc to add them first; `zuhttp`'s
  `zu_body_stream()` already allows full duplex. No gzip, no retry. Its
  R API may still change before its first CRAN release; any gap
  `zuspark` finds is a zugrpc issue.
- **`zuhttp`** (0.1.0 on `main`, not on CRAN, `v1.0` #22 open): the
  indirect hard dependency. HTTP/2 goes into `zuhttp` as a post-1.0
  minor, nghttp2 vendored there, ALPN `h2` over its native TLS and `h2c`
  in the clear, so a Spark Connect server on loopback and a Databricks
  workspace on 443 are both reachable. Stream cancellation
  (`RST_STREAM`) and TLS trust are `zuhttp`'s. `zuspark` does not import
  it directly in v0.1.0; the Databricks control-plane calls of §14 would.
- **`zuproto`** (RFC 0010, 0.1.0 on `main`, CRAN submission open):
  `proto_pool()` over the shipped descriptor set, `proto_encode()` for
  requests, `proto_decode()` for responses with `int64 = "double"` for
  the counters and `bytes = "raw"` for `ArrowBatch.data`. It has no C
  API (its §3), and `proto_compile()` is "later, when asked" (its §2;
  issue #73 open), so the descriptor set is built by a `tools/` script
  with `protoc`, never at install time (D4).
- **`zuarrow`** (RFC 0041; `0.1.0` on `main` since 2026-11-04, Stages 0
  to 10 merged, 11 the review open). It provides:
  - `arrow_read()` over the result's IPC messages: a list of
    `ArrowBatch.data` raw vectors read as one stream (pedrobtz/zuarrow#90,
    2026-11-04), which checks that every message repeats the first
    one's schema;
  - `arrow_to_r()` and the `lossy = "error"` default (its D4);
  - handles that are nanoarrow's (its D6).

  IPC compression, should a server send it, is `zuarrow`'s through
  `zukomp` 0.2.0, the release with the codecs.
- **`zuid`** (RFC 0043, 0.1.0 on `main`, CRAN submission open):
  `uuid(version = 4)` for the session id and for operation ids
  (the function is `uuid()`, not `id_uuid()`; corrected 2026-11-05).
- **`zuint`, `decimal`, `zutempo`**: `Suggests`. They register the exact
  targets for `int64`, `decimal128` and microsecond timestamps in
  `zuarrow`'s registry; without them `spark_collect()` errors or, with
  `lossy = "character"`, returns text (§7.7).
- **`zujson`**: `Suggests`, for `print()` of `messageParameters`.
- **`azr`**: not used; Databricks control-plane calls are §14, on the
  same `zuhttp`.
- **`zulake`, `qio`, `duckdb`**: the local path. A Delta table a Spark
  job wrote is read on a laptop by `zulake` and `qio`; `zuspark` reads
  it where the cluster is. Both return Arrow.

## 4. Dependencies and the release gate

The chain is `zuspark` → `zugrpc` (pure R) → `zuhttp` (HTTP/2), so the
gate has five parts:

1. **`zuhttp` with HTTP/2** (its milestone M4, tracker zuhttp#128,
   work packages zuhttp#129 to #131): crandb `not_found`; `DESCRIPTION`
   0.1.0 on `main`, `v0.1.0 CRAN submission` (#40) and `v1.0` (#22)
   open, and HTTP/2 is a minor after 1.0. The longest pole.
2. **`zugrpc`**: no repository on 2026-11-03; it needs the release above
   before its own Stage 0.

*Update 2026-11-05:* zuhttp#128 to #131 (HTTP/2, trailers, cancel,
streaming bodies) are closed and on `zuhttp` `main`, whose `DESCRIPTION`
is still 0.1.0. `pedrobtz/zugrpc` exists (`main` `ba9d83b`) and runs
against live Spark on that `main`; its Stage 7 (release) waits on
zuhttp#40 and zuproto#13. So the HTTP/2 pole is a CRAN release of
code that already exists, not unbuilt work; which `zuhttp` version
reaches CRAN with it is zuhttp#40's to say (Q5).
3. **`zuproto` 0.1.0**: crandb `not_found`; Stages 0 to 10 (#2 to #12)
   closed, `CRAN-submission` (#13) and `v0.1.0` (#1) open.
4. **`zuarrow`**: crandb `not_found`; `DESCRIPTION` 0.1.0 on `main`;
   Stages 0 to 10 closed; Stage 11 (review, #29) and `CRAN-submission`
   (#12) open. It builds on `zukomp`'s development version through
   `Remotes:` and submits after `zukomp` 0.2.0, the release with the lz4
   and zstd codecs, reaches CRAN, which can happen 2026-11-27 at the
   earliest.
5. **`zuid` 0.1.0**: crandb `not_found`; Stages 0 to 11 closed,
   `CRAN-submission` (#15) open.

*Decided 2026-11-06 (Q6):* 0.1.0 does not wait for this gate. It is a
GitHub release that keeps `Remotes:`, with floors only where a sibling
has a tagged release (`zuproto` and `zuid` 0.1.0); `zugrpc` and
`zuarrow` are untagged at 0.0.0.9000 and come from their `main`. The
gate below is the CRAN version's.

Development uses `Remotes:` for the four `Imports`; `zugrpc`'s own
`Remotes:` reaches `zuhttp`. Release is not a stage: after the last stage the
package waits in `Pre-flight` until the gate holds, drops `Remotes:` and
submits. `zukomp` reaches `zuspark` only through `zuarrow` and gates it
only by gating `zuarrow`: `zuarrow` needs 0.2.0, not the 0.1.0 on CRAN
since 2026-10-16.

## 5. The protocol, as zuspark uses it

Read from `sql/connect/common/src/main/protobuf/spark/connect/` at
`v4.3.0` (tag commit 2026-10-27, the latest tag on 2026-11-03). Eleven
files, about 169 KB: `base.proto` 48,795 bytes, `relations.proto`
41,113, `expressions.proto` 19,043, `commands.proto` 18,604,
`pipelines.proto` 14,233, `catalog.proto` 7,673, `types.proto` 5,793,
`common.proto` 4,769, `ml.proto` 4,675, `ml_common.proto` 2,073,
`example_plugins.proto` 1,295. All go into the descriptor set; v0.1.0
builds messages from `base.proto` only, plus `Relation{sql}` from
`relations.proto` and `Command{sql_command}` from `commands.proto`, and
decodes `DataType` from `types.proto` in schema answers.

`SparkConnectService` has twelve RPCs at `v4.3.0`: `ExecutePlan`
(server-streaming), `AnalyzePlan`, `Config`, `AddArtifacts`
(client-streaming), `ArtifactStatus`, `Interrupt`, `ReattachExecute`
(server-streaming), `ReleaseExecute`, `ReleaseSession`,
`FetchErrorDetails`, `CloneSession` and `GetStatus`. `v3.4.0` has the
first four; `v3.5.0` adds `ArtifactStatus`, `Interrupt`,
`ReattachExecute` and `ReleaseExecute`; `v4.0.0` adds `ReleaseSession`
and `FetchErrorDetails`. v0.1.0 calls eight: all but `AddArtifacts`,
`ArtifactStatus`, `CloneSession` and `GetStatus`.

`ExecutePlanResponse` carries `session_id`, `server_side_session_id`,
`operation_id`, `response_id`, a sixteen-member `response_type` oneof,
and `metrics`, `observed_metrics` and `schema` beside it. v0.1.0 reads
three members and skips the rest by name:

```text
ArrowBatch        { int64 row_count; bytes data;
                    optional int64 start_offset;
                    optional int64 chunk_index;
                    optional int64 num_chunks_in_batch; }
SqlCommandResult  { Relation relation; }
ResultComplete    { }
```

Every `ArrowBatch.data` is a complete Arrow IPC stream that starts with
the schema (`ArrowConverters.toBatchWithSchemaIterator`: "each output
arrow batch starts with the schema"), so each batch is decodable on its
own. Batches are cut at `spark.sql.execution.arrow.maxRecordsPerBatch`
rows or 70 % of `spark.connect.grpc.arrow.maxBatchSize` bytes. The
`chunk_index` fields serve `ResultChunkingOptions`, which v0.1.0 does
not request, so a batch always arrives whole.

## 6. Public R API (v0.1.0 shape)

```r
# sessions
spark_connect(url = Sys.getenv("SPARK_REMOTE"), ...,
              user_id = NULL, session_id = NULL, user_agent = NULL,
              headers = NULL, deadline = NULL)
spark_disconnect(con, allow_reconnect = FALSE)
spark_version(con)                 # AnalyzePlan.SparkVersion
spark_config(con, ...)             # get all, get by name, set, unset
spark_session_id(con)              # client and server-side ids

# SQL
spark_sql(con, sql)                # a zuspark_query, or a data frame
                                   # for DDL, DML and scripts
spark_schema(x)                    # AnalyzePlan.Schema, Spark's types
spark_collect(x, lossy = "error", to = NULL)   # one data frame
spark_fetch(x)                     # a zuarrow stream
spark_execute(con, sql)            # run, discard rows, count them
spark_interrupt(con, x = NULL)     # one query, or every operation

zuspark_info()
```

`spark_sql()` returns a `zuspark_query`: the connection, the relation
the server answered with (§7.3), and no rows. `spark_collect()` and
`spark_fetch()` execute it; each call is a fresh execution on the
server. When the statement was a command, the server has already run it
and `spark_sql()` returns its rows as a data frame at once.

`spark_fetch()` returns a `zuarrow` stream handle over the result's
batches. It reads the gRPC stream in R, then decodes the messages with
`arrow_read(list(...), lazy = TRUE)` (decided 2026-11-04, §7.5). The
stream does not pull from gRPC on each `get_next()`: that would mean C
calling R, which zuarrow forbids, from whatever thread the consumer uses.
`spark_collect()` is `arrow_to_r()` over that stream; `to` and `lossy` pass through to
`zuarrow` (§7.7). `as.data.frame()` on a query is `spark_collect()`.

*Decided 2026-11-05 (Stage 3, with the maintainer):* `spark_schema(x)`
returns Spark's schema, not an Arrow one: one `AnalyzePlan.Schema` call,
no execution, as a data frame of `name`, `type` (Spark SQL's DDL text,
such as `decimal(10,2)` or `array<struct<k:string>>`) and `nullable`.
The server answers with Spark's `DataType`, and turning that into Arrow
would repeat the server's own mapping, which D10 rules out; the Arrow
schema is the schema of `spark_fetch()`'s stream.

*Decided 2026-11-05 (Stage 2, with the maintainer):* `spark_fetch(x)`
takes no `lossy` or `to`. A lazy `zuarrow` handle cannot carry them
(`arrow_read(lazy = TRUE)` refuses them), and they belong to whoever
converts the stream: `arrow_next()`, `arrow_to_r()`, `as.data.frame()`;
duckdb and nanoarrow ignore them. `spark_collect(x, lossy, to)` keeps
both and is `arrow_to_r(spark_fetch(x), to, lossy)` (D6).
`spark_execute()` returns the number of rows the statement produced,
invisibly; a query's metrics, observed metrics, row count, reattaches
and skipped responses of its last run are kept on the `zuspark_query`,
which is an environment for that reason. A command answers with a
`local_relation` whose `data` is one IPC stream, a schema-only one when
the command has no output; an empty query result is one batch of zero
rows that still carries the schema (both checked on 4.2.1).

*Decided 2026-11-05 (Stage 1):* `spark_config(con)` returns every
setting as a named character vector; unnamed strings read those
settings (`NA` for one without a value); `name = value` sets, a named
`NULL` or `NA` unsets, and returns the previous values invisibly, so
that passing them back restores the session, as with `options()`.
Mixing reads and sets in one call is an error. The server's `warnings`
become R warnings. `spark_version()` is the string the server reported
at connect, such as `"4.2.1"`, cached on the connection.
`spark_disconnect(allow_reconnect = FALSE)` maps to
`ReleaseSessionRequest.allow_reconnect`: Spark 4.2.1 retires a released
id, and a later connect with it fails with
`INVALID_HANDLE.SESSION_CLOSED` (status `INTERNAL`), unless the release
allowed reconnects.

## 7. Semantics

### 7.1 Connection strings

`client-connection-string.md` at `v4.3.0` fixes the grammar, and
`DefaultChannelBuilder` implements it by rewriting `sc://` to `http://`
and handing the result to Python's URL parser:

```text
sc://host:port/;param1=value;param2=value
```

- The scheme is `sc://`; the path must be empty or `/`; parameters
  follow the first `;` as `key=value` pairs split on `;`,
  percent-decoded, case-sensitive. An IPv6 host is bracketed. The
  default port is 15002.
- Reserved parameters: `token` (bearer auth, and turns TLS on),
  `use_ssl` (default `false`), `user_id` (fills `UserContext.user_id`),
  `user_agent` (prefixes `client_type`), `session_id` (must parse as a
  UUID), `grpc_max_message_size` (default 128 MiB) and four
  `grpc_keepalive_*` keys: `_enabled`, `_time_ms`, `_timeout_ms` and
  `_without_calls`.
  *Decided 2026-11-05 (Stage 1):* `session_id` accepts any well-formed
  UUID and is sent as written. PySpark's `uuid.UUID(x, version=4)`
  rewrites the version bits rather than checking them, so it never
  rejected another version. The four keepalive keys are reserved as at
  `v4.3.0`, though `v4.2.1` reserves only the first five names; they map
  to `zugrpc::grpc_keepalive()` (`_enabled=false` turns it off), and
  `_without_calls` is accepted and ignored, since zugrpc's keepalive
  pings only before a call.
- **Every other parameter becomes a request header**, sent as gRPC
  metadata on every call (`ChannelBuilder.metadata()`; the Go client's
  `channel.go` states the same rule). That is how
  `x-databricks-cluster-id=…` reaches a server without the client
  knowing the name.
- `SPARK_REMOTE` is the default URL and `SPARK_CONNECT_AUTHENTICATE_TOKEN`
  the fallback token, both read by PySpark and by `zuspark`. `user_id`
  defaults to `SPARK_USER`, then `USER`, then (on Windows, which sets
  neither) `Sys.info()[["user"]]`; `session_id` to a fresh version 4
  UUID from `zuid`; the user agent to `SPARK_CONNECT_USER_AGENT`, as in
  PySpark, then `zuspark`.

`spark_connect()` takes the string, `...` as named parameters appended
to it, and `headers` for names that are not valid in a URL. A malformed
string is a `zuspark_url_error` naming the part, with PySpark's four
messages as the test cases. *2026-11-05:* PySpark's missing-host
message quotes its internal `http://` rewrite of the string; zuspark's
quotes the string as given. A malformed port, which Python reports as a
`ValueError`, is a fifth `zuspark_url_error` (part `port`).

### 7.2 Sessions

A session is a pair: the client's `session_id` and the server's
`server_side_session_id`, which arrives on every response. After the
first response the client sends it back as
`client_observed_server_side_session_id` on every request, so a server
restart that recycled the client id is detected by the server rather
than by a wrong result. `client_type` is the user agent in PySpark's
layout: the `user_agent` parameter or `zuspark`, then `zuspark/<ver>`,
`spark/<pinned tag>`, `os/<sysname>` and `r/<R version>`,
space-separated as PySpark's, with the user agent at most 2,048
characters once percent-encoded (the value itself is sent as written,
as PySpark sends it).

The client also checks both ids on every response (*decided
2026-11-05*, after PySpark's response-integrity check): a `session_id`
that is not the client's, or a `server_side_session_id` different from
the first one seen, is a `zuspark_session_error`, as is any call on a
released connection.

`spark_connect()` sends one `Config` with `get_all` to prove the channel,
the credential and the session, and one `AnalyzePlan.SparkVersion`. A
server older than 3.5.0 is refused with `zuspark_version_error` (D7): it
lacks `Interrupt`, reattachable execution and `ReleaseExecute`, and its
`SqlCommand` has no `input` field (§7.3). A 3.5 server works with two
RPCs missing: `ReleaseSession` and `FetchErrorDetails` return
`UNIMPLEMENTED`, which `spark_disconnect()` and the error path treat as
"not available", not as failure.

`spark_disconnect()` sends `ReleaseSession` and closes the channel; a
finalizer does the same best-effort, and the server drops an idle
session after `spark.connect.session.manager.defaultSessionTimeout`,
sixty minutes by default.

*2026-11-06 (#26):* the gRPC channel, whose headers hold the bearer
token, lives in a registry in zuspark's namespace, keyed by a fresh id
per connection; the connection holds only the key. Queries and Spark
errors keep the connection, and are saved or sent between processes by
callr, future, mirai or targets; with the channel on the connection the
token went with them. A connection unserialised in another R session has
no channel there, and every call on it is a `zuspark_session_error`.

### 7.3 Executing SQL: the two-step protocol

In Spark 3.4 a query was a `Relation{sql}` sent as `Plan.root`. Since
3.5 PySpark sends SQL as a **command** so that DDL and DML run eagerly,
and in 4.x the shape is:

```text
Plan { command { sql_command { input: Relation { sql { query } } } } }
```

`SqlCommand.sql`, `args`, `pos_args` and the two argument maps are
`[deprecated = true]` at `v4.3.0`; `input` (field 6) carries the
relation. `v3.4.0` has only `sql` and `args`, `v3.5.0` adds `pos_args`;
neither has `input`, which is one reason D7 draws the floor at 3.5 and
accepts that a 3.5 server needs the deprecated `sql` field (Q2).

The server's `handleSqlCommand` executes the statement and answers with
one `SqlCommandResult`, in one of two forms:

1. **A command or a SQL script** (DDL, DML, `SET`, `SHOW`, …): the rows
   it produced as a `Relation{local_relation{data}}` whose `data` is one
   Arrow IPC stream, followed by a `Metrics` response. `spark_sql()`
   decodes that stream and returns a data frame.
2. **A query** (`SELECT`, `WITH`, `VALUES`, …): the relation echoed
   back, `Relation{sql{query}}`, unexecuted. `spark_sql()` keeps it as
   the `zuspark_query`'s plan. `spark_collect()` then sends a second
   `ExecutePlan` with `Plan{root = <that relation>}` and reads
   `ArrowBatch` responses until `ResultComplete`.

This is PySpark's behaviour (`session.py`: `execute_command()`, then
`DataFrame(CachedRelation(sql_command_result))`), so a `zuspark_query`
is to `spark_sql()` what a lazy `DataFrame` is to `spark.sql()`.
`spark_execute()` is form 2 with the rows discarded.

### 7.4 Reattachable execution

Every `ExecutePlan` of v0.1.0 carries a client-made `operation_id` (a
`zuid` UUID) and `ReattachOptions{reattachable: true}`. The server then
buffers responses and may close the stream **without** `ResultComplete`
after `spark.connect.execute.reattachable.senderMaxStreamDuration` (two
minutes) or `senderMaxStreamSize` (1 GiB). The client's loop, after
`reattach.py`:

- a stream that ends without `ResultComplete` is reattached with
  `ReattachExecuteRequest{operation_id, last_response_id}`;
- a stream that fails before any response is re-sent as a new
  `ExecutePlan` only when reattach answers
  `INVALID_HANDLE.OPERATION_NOT_FOUND`, since the request never arrived;
- after consumed responses the client sends
  `ReleaseExecute{release_until{response_id}}`; after `ResultComplete`,
  or on abandoning a query, `ReleaseExecute{release_all}`.

PySpark releases from a thread pool. `zuspark` has no threads: it
releases every `k` responses inline on the same channel (`k` chosen at
Stage 2, 16 to start), and always on completion. Nothing else is
retried in v0.1.0 (D8).

*Decided 2026-11-05 (Stage 2):* `k` is 16. A cut stream reattaches;
three reattaches in a row that bring no new response are a
`zuspark_protocol_error`, so a server that keeps ending streams cannot
loop forever. A transport failure (`zugrpc_error_transport`) mid-stream
counts as a cut; any other status ends the execution. Measured against
Spark 4.2.1 with `senderMaxStreamDuration=1s` and
`senderMaxStreamSize=1m`: `range(10^8)` reattached 787 times and arrived
whole in 65 s. The server buffers a result until the client releases
it, and produces faster than a client reads, so that run needs a driver
heap above Spark's default 1 GiB (3 GiB in `live.yaml`); PySpark meets
the same limit.

*Found by zugrpc's tests against Spark 4.2.1, 2026-11-05:* Spark sends
`ResultComplete` only when the request sets reattachable execution in
`request_options`, which D8 always does; a stream without it simply
ends. Those tests sent `ExecutePlanRequest` with `session_id` (a UUID),
`user_context$user_id` and `plan$root`, all required; the `command`
form of §7.3 is untested there. Long reattach loops are untested in
zugrpc.

### 7.5 Result batches

`spark_fetch()` collects the `ArrowBatch.data` raw vectors in R and
decodes them in one call, `arrow_read(batches, lazy = TRUE)`, which reads
a list of IPC streams as one (pedrobtz/zuarrow#90). Every batch repeats
the schema, and zuarrow checks each against the first; a mismatch is
its `zuarrow_format_error`, which `zuspark` rethrows as a
`zuspark_protocol_error`. So is a `row_count` that disagrees with
the decoded rows or a `start_offset` that is not the running total. A
`response_type` the client does not know is skipped and counted, so a
newer server's extra messages do not break a result. `metrics` and
`observed_metrics` are kept on the query object for `summary()`.

### 7.6 The DuckDB hand-off

`spark_fetch()` returns a `zuarrow` stream handle, which is a nanoarrow
`ArrowArrayStream` (zuarrow D6). `duckdb::duckdb_register_arrow()` takes
such a stream, so a Spark result joins a local DuckDB table without
passing through an R data frame:

```r
q <- spark_sql(con, "SELECT id, ts, amount FROM sales WHERE day = 7")
duckdb::duckdb_register_arrow(local, "sales7", spark_fetch(q))
DBI::dbGetQuery(local, "SELECT sum(amount) FROM sales7")
```

*Checked 2026-11-04, duckdb 1.5.5:* `duckdb_register_arrow()` only
accepts the `arrow` package's objects. It calls `arrow::Scanner` and
`export_to_c()`, so the example above needs `arrow` installed and a
conversion to an `arrow` reader. DBI's `dbWriteTableArrow()` and
`dbAppendTableArrow()` take a `zuarrow` stream, but duckdb has no method
for them. DBI's default converts each batch to an R data frame through
`nanoarrow`, which is neither zero-copy nor exact. The hand-off without
`arrow` waits for duckdb to accept a C stream directly. The benchmark
row in §10 holds only with `arrow`.

### 7.7 Types

The Spark-to-Arrow mapping is the **server's**, `ArrowUtils.toArrowType`
at `v4.3.0`, with the session's `spark.sql.session.timeZone` as the
zone and `spark.sql.execution.arrow.useLargeVarTypes` deciding the
large variants:

<!-- markdownlint-disable MD013 -->

| Spark | Arrow (from the server) | R, through zuarrow |
| --- | --- | --- |
| `BOOLEAN` | `bool` | `logical` |
| `TINYINT`, `SMALLINT`, `INT` | `int8`, `int16`, `int32` | `integer` |
| `BIGINT` | `int64` | `zuint` int64; else error, or text |
| `FLOAT`, `DOUBLE` | `float`, `double` | `double` |
| `STRING` | `utf8`, or `large_utf8` | `character` |
| `BINARY` | `binary`, or `large_binary` | list of `raw` |
| `DECIMAL(p, s)` | `decimal128(p, s)` | `decimal`; else error, or text |
| `DATE` | `date32` | `Date` |
| `TIMESTAMP` | `timestamp[us, <session zone>]` | `zutempo`; else error, or text |
| `TIMESTAMP_NTZ` | `timestamp[us]`, no zone | `zutempo`; else error, or text |
| `INTERVAL DAY TO SECOND` | `duration[us]` | `zutempo`; else error, or text |
| `INTERVAL YEAR TO MONTH` | `interval[year_month]` | `zutempo`; else error |
| `INTERVAL` (calendar) | `interval[month_day_nano]` | `zutempo`; else error |
| `ARRAY<T>`, `MAP<K, V>`, `STRUCT` | `list`, `map`, `struct` | list and data frame columns |
| `NULL` | `null` | `logical` of `NA` |
| `VARIANT` | `struct{value: binary, metadata: binary}` | that struct, undecoded |
| `TIME(p)` (Spark 4.1+) | none: Spark 4.2.1 refuses (`UNSUPPORTED_TIME_TYPE`) | the server's error |

<!-- markdownlint-enable MD013 -->

*Checked 2026-11-05 (Stage 3) against Spark 4.2.1 and PySpark 4.2.1:*
the server sends exactly the Arrow types above, with
`spark.sql.session.timeZone` as the timestamp zone (`Etc/UTC` by
default). `TIME` is decided by the server: it refuses to send a `TIME`
column as Arrow (`UNSUPPORTED_TIME_TYPE`, SQLSTATE `0A000`), though
`spark_schema()` reports it as `time(p)`; zuspark passes the error on.
PySpark's own `toArrow()` refuses the two interval types without a
`duration` form (`UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION`), so for
those the server is the only reference; `zuarrow` has no R form for
them, even as text. `CHAR(n)` arrives as `string`. *Same day, later:*
`zuarrow`'s `main` (825c43b) now reads `timestamp[us]`, with or
without a zone, as `POSIXct` under every `lossy` policy, so the
`TIMESTAMP` and `TIMESTAMP_NTZ` rows above are `POSIXct` with that
`zuarrow`; the tests accept either rule, since the rule is `zuarrow`'s
(D10). None of `zuint`,
`decimal` and `zutempo` registers its exact targets with `zuarrow` yet
(zuint#52, decimal#24, zutempo#94), so the "exact with" column holds
only once they do; until then `lossy = "character"` gives exact text
and `lossy = "allow"` the widened value. *Decided 2026-11-06:* exact
conversion is not part of 0.1.0; it comes in a later version, once the
three packages register, with no change to zuspark (D10).

`zuspark` adds nothing to this table: `zuarrow`'s exact-or-error rule
(its §6.3) is the R edge. A `BIGINT` column with a value beyond 2⁵³ in
a session without `zuint` is a `zuarrow_lossy_error` naming `zuint`;
`lossy = "character"` gives its digits; `lossy = "allow"` a double.
`TIMESTAMP` carries the server's session zone, which
`spark_config(con, spark.sql.session.timeZone = "UTC")` sets before the
query; `zuspark` never converts a timestamp itself (D10).

What v0.1.0 refuses, by letting `zuarrow` refuse it: the three interval
types without `zutempo` under `lossy = "error"`, and any Arrow type
`zuarrow` does not decode. `VARIANT` arrives as its two binary fields
and is not decoded (§14). Nothing is sent to the server as data in
v0.1.0, so the reverse mapping does not arise.

### 7.8 Databricks Connect

Databricks Connect for Databricks Runtime 13.3 and later is Spark
Connect: its documentation says a cluster's "Databricks Connect (and the
underlying Spark Connect) services" can be disabled with
`spark.databricks.service.server.enabled false`, and configures the
client with `host`, `token` and `cluster_id`, or
`serverless_compute_id = auto`, which "ignores the `cluster_id`". It
documents no `sc://` string and no header name (read 2026-11-03), and
`pysparklyr` never builds one either.

What this RFC therefore commits to: a bearer token through `token=`,
TLS on port 443, and any header the workspace needs through the
unreserved-parameter rule of §7.1, so
`sc://<workspace>:443/;token=…;x-databricks-cluster-id=…` works if that
is the header's name, without `zuspark` knowing it. Whether it is, what
serverless needs (an `x-databricks-session-id`, per the maintainer's
recollection) and what the workspace expects in `user_agent` are Stage
5's questions (Q3). A `spark_connect_databricks(host, token,
cluster_id)` convenience follows once they are answered.

**Decided 2026-11-06** (Q3, checked against a Databricks Free Edition
workspace, details in [databricks.md](databricks.md)): the endpoint is
`sc://<workspace>:443/;use_ssl=true` with the token as bearer.
Serverless compute requires an `x-databricks-session-id` header, and
refuses every call without one ("Invalid compute mode headers"); any
UUID is accepted, and zuspark sends the session id. A cluster is chosen
with `x-databricks-cluster-id` (the name is confirmed by the server's
`NOT_FOUND` for a made-up id; no real cluster was run). `user_agent`
needs nothing Databricks-specific. Since the header carries a value
zuspark generates, the convenience is warranted:
`spark_connect_databricks(host, token, cluster_id = NULL, ...)` reads
`DATABRICKS_HOST` and `DATABRICKS_TOKEN` by default, takes the host as
the browser shows it, and adds the session header for serverless or the
cluster header for a cluster. `spark_connect()` is unchanged: the same
session works through it with `headers =`.

### 7.9 Interrupts and deadlines

An R interrupt while a stream is being read cancels the gRPC call
through `zugrpc`, which is `zuhttp` sending `RST_STREAM`, and then sends
`InterruptRequest{interrupt_type: INTERRUPT_TYPE_OPERATION_ID,
operation_id}`, so the server stops the job rather than only the
transfer; the response's `interrupted_ids` is reported.
`spark_interrupt(con)` with no query sends `INTERRUPT_TYPE_ALL`.
`deadline` is the per-call gRPC deadline, sent as `grpc-timeout`; a
streaming call's deadline covers the whole stream, so the default is
none, and `Config`, `AnalyzePlan` and `Interrupt` get thirty seconds.

*From zugrpc, 2026-11-05:* a deadline is seconds or a `POSIXct`. On a
stream it is checked before each read, so a server that stalls
mid-stream can overrun it by up to one deadline until zuhttp#173 adds a
per-read timeout; there is no `$next_message(timeout =)`. A local timeout
is `DEADLINE_EXCEEDED`. Ctrl-C during a read raises
`zugrpc_error_interrupt`, and `$cancel()` cancels the stream; a real
Ctrl-C has so far only been mocked there, so Stage 4 is its first live
test.

## 8. Errors

Every condition inherits from `zuspark_error`; tests assert on class.

<!-- markdownlint-disable MD013 -->

| Class | Raised when |
| --- | --- |
| `zuspark_url_error` | a connection string that fails §7.1 |
| `zuspark_version_error` | a server below 3.5.0 (D7) |
| `zuspark_rpc_error` | a non-OK gRPC status with no Spark `ErrorInfo` |
| `zuspark_sql_error` | a Spark error: `ErrorInfo` present; carries class, SQLSTATE, parameters |
| `zuspark_session_error` | a server-side session id that changed, or a released session used |
| `zuspark_protocol_error` | a response that contradicts the protocol (§7.5) |
| `zuspark_interrupt_error` | the query was interrupted; carries `interrupted_ids` |

<!-- markdownlint-enable MD013 -->

A failed call carries `google.rpc.Status` in the
`grpc-status-details-bin` trailer, which `zuproto` decodes from the
shipped `status.proto` and `error_details.proto`. Spark's
`ErrorUtils.buildStatusFromThrowable` at `v4.3.0` puts one
`google.rpc.ErrorInfo` in it with `reason` the exception's class name,
`domain` `org.apache.spark`, and metadata keys:

- `classes`: a JSON array of the exception's class hierarchy, always;
- `sqlState` and, when they fit `spark.connect.grpc.maxMetadataSize`,
  `errorClass` and `messageParameters` (JSON), for a `SparkThrowable`;
- `errorId`, when `spark.sql.connect.enrichError.enabled` is on, keyed
  to the exception on the server for `FetchErrorDetails`;
- `stackTrace`, abbreviated, when
  `spark.sql.pyspark.jvmStacktrace.enabled` is on.

`zuspark_sql_error` exposes `message`, `error_class`, `sql_state`,
`message_parameters` and `classes`, and keeps `errorId`.
`spark_error_details(err)` sends `FetchErrorDetailsRequest{error_id}` and
returns the response's `errors` (type hierarchy, message, stack frames,
`cause_idx` chain, `spark_throwable` with `query_contexts`) as a list of
data frames, on request only (D9).

*Decided 2026-11-05 (Stage 4):* a zugrpc condition becomes a
`zuspark_sql_error` when its status details carry Spark's `ErrorInfo`
(domain `org.apache.spark`), and a `zuspark_rpc_error` otherwise,
transport failures and `DEADLINE_EXCEEDED` included; both keep zugrpc's
classes after `zuspark_error` (for example `zugrpc_error_internal`) and
the zugrpc condition as `parent`, so a handler for either layer works.
`zuspark_sql_error`'s fields are `error_class`, `sql_state`,
`message_parameters` (named character), `classes`, `error_id`, `reason`
and `grpc_status`; it also keeps the connection, so that
`spark_error_details(err)` needs no other argument (`con` may still be
given). `spark_error_details()` returns a list, root exception first, of
lists with `type`, `message`, `error_class`, `sql_state`,
`message_parameters`, `cause` (an index into the list), and `stack` and
`contexts` as data frames. Spark's two JSON values are read by a small
parser of flat string arrays and objects, so `zujson` is not needed.
Checked live: a typo is `PARSE_SYNTAX_ERROR`, `42601`, with parameter
`error = 'SELEC'`.

*What zugrpc raises, 2026-11-05:* a non-OK status is
`zugrpc_error_status` plus one class per code (`zugrpc_error_internal`,
`zugrpc_error_unavailable`, …), with fields `grpc_status`, `grpc_code`,
`grpc_message`, `details`, `http_status`, `method`, `headers` and
`trailers`. The decoded `ErrorInfo` is `cnd$details$details$ErrorInfo`:
`$reason`, `$domain`, and `$metadata` keyed `errorClass`, `sqlState`,
`errorId`, `messageParameters`, `classes`. A transport failure is
`zugrpc_error_transport`, also classed `zugrpc_error_unavailable`. A
missing table, tested against Spark 4.2.1, is status `INTERNAL` with
`errorClass` `TABLE_OR_VIEW_NOT_FOUND` and `sqlState` `42P01`. These are
the inputs §8's classes are built from; `errorId` is what
`spark_error_details()` sends.

## 9. Testing

`testthat` 3e, one file per section of §7.

- **Recorded fixtures, every CI leg.** Each RPC of §5 has a recorded
  request and response in `tests/testthat/fixtures/`, captured once from
  the live container as raw protobuf bytes and IPC payloads. A fake
  channel (a function from request bytes to recorded response bytes,
  injected through an internal option) replays them, so session
  lifecycle, the two-step SQL protocol, reattach after a cut stream,
  release, error decoding and interrupts run with no network on macOS,
  Windows, Ubuntu and the r-devel containers.
- **Proto snapshots.** Every SQL call's encoded `Plan` is snapshot as
  hex, decoded back through `proto_to_json()` beside it, so a change to
  the request shape is a readable diff.
- **Live Spark, Ubuntu legs only.** A job starts
  `apache/spark:4.2.1-scala2.13-java17-python3-ubuntu` (the newest
  Docker Hub tag on 2026-11-03; `apache/spark-docker` has no 4.3.0
  directory yet, Q1) with `/opt/spark/sbin/start-connect-server.sh
  --wait` and runs the suite against `sc://localhost:15002`. Spark 4.x
  bundles Spark Connect, so no `--packages` is needed, and runs on Java
  17, 21 or 25 (`docs/index.md`). The other legs cannot run a JVM
  container, which is why the fixtures exist.
  *Working recipe, 2026-11-05:* zugrpc's `.github/workflows/live.yaml`
  `spark` job runs `apache/spark:4.2.1` with `SPARK_NO_DAEMONIZE=true`
  and `start-connect-server.sh --conf
  spark.connect.grpc.binding.address=0.0.0.0`, waits for the log line
  "Spark Connect server started" on port 15002, and passes the address
  to tests in `ZU_SPARK_TEST_SERVER`. zuspark's live job starts from it.
- **Type matrix against PySpark.** In the container, `pyspark` runs one
  `SELECT` per row of §7.7's table and writes the Arrow IPC bytes;
  `zuspark` must decode the same bytes to the same schema and values
  (`arrow_equal()`), and `spark_collect()` must error, text or widen
  exactly as `lossy` says, with and without `zuint`, `decimal` and
  `zutempo` loaded.
- **Reattach.** With the server's `senderMaxStreamDuration` set to one
  second, a `SELECT` over `range(10^8)` must reattach at least once and
  return every row once.
- **Hostile responses.** Truncations, byte flips and oversized counts in
  recorded responses give `zuspark_protocol_error`, never a crash; the
  decoders are `zuproto`'s and `zuarrow`'s, both fuzzed already.
- **From the family** (R3): `test-abi.R` asserts there is no `src/`.

## 10. Performance targets

Measured by `tools/run-benchmarks` against PySpark 4.x in the same
container on the same machine, recorded in `.agents/benchmarks.md`.

| Operation | Target |
| --- | --- |
| `spark_connect()` on loopback | under 100 ms |
| `SELECT 1`, round trip | within 2× of `spark.sql("SELECT 1").collect()` |
| 10⁷ rows × 4 numeric columns, `spark_collect()` | within 1.5× of `toArrow()` |
| the same through `spark_fetch()` into DuckDB, with `arrow` (§7.6) | no R-heap copy of the rows |
| memory above the result for `spark_collect()` | under one batch |

Guesses until Stage 2 measures PySpark; the transport and decoding are
the siblings', so a missed target is theirs to profile and this RFC's
to record.

*Measured 2026-11-06 (Stage 6), in [benchmarks.md](benchmarks.md):*
connect (65 ms) and the round trip (0.92× of PySpark) are met. The
10⁷-row collect is 1.78× of `toArrow()`, in zugrpc's stream reading
(zugrpc#55), and 0.76× of `toPandas()`. The two memory rows are not met
by §7.5's design, which holds every batch's IPC bytes in R before
decoding; batch-by-batch decoding is §14's chunked results.

## 11. Roadmap

The stages, their order, exit criteria and CI tiers are in
[roadmap.md](roadmap.md), moved there on 2026-11-05.

## 12. Acceptance criteria for v0.1.0

- Every behaviour in §7 and §8 has a passing test on the recorded
  fixtures on every r-actions leg, and on live Spark on the Ubuntu legs.
- §7.7's matrix matches PySpark's bytes and schema for every row.
- A cut stream is reattached and every row arrives exactly once.
- No hostile response of §9 crashes; `zuspark` has no `src/`.
- `R CMD check --as-cran` is clean on all r-actions legs with no network
  access; `Remotes:` gone; the descriptor set's tag and hash recorded.
- Each stage of [roadmap.md](roadmap.md) is one pull request; a stage
  needing a second moves scope to §14, not the release.

## 13. Decisions

All are **(C)**: this RFC decides them and the maintainer may overturn
any of them.

- **D1 (C).** Admit `zuspark` as an application package on §1.3's point
  4, accepting that it fails point 3.
- **D2 (C).** SQL only in v0.1.0: one relation type, one command type.
  A plan builder is a package's worth of API, and `dbplyr` already
  generates Spark SQL.
- **D3 (C).** Pure R, no `src/`. HTTP/2 and TLS are `zuhttp`'s, gRPC
  framing is `zugrpc`'s (also pure R), protobuf is `zuproto`'s, Arrow is
  `zuarrow`'s; what remains is sequencing, which R does well enough and
  tests more easily. The cost is §4: the gate waits on `zuhttp`'s
  HTTP/2 minor.
- **D4 (C).** Ship a compiled descriptor set at a pinned Spark tag
  (`v4.2.1`, per Q1), built by a `tools/` script with `protoc`, not
  at install time and not through `proto_compile()`, which `zuproto`
  does not have. Re-pinning is a stage-less pull request that re-runs
  the snapshots.
- **D5 (C).** No DBI import. A DBI shim is §14; the verbs are DBI-shaped
  so that the shim is thin.
- **D6 (C).** Results are `zuarrow` handles first and data frames
  second; `spark_collect()` is `arrow_to_r()` over `spark_fetch()`.
- **D7 (C).** Server floor 3.5.0. Spark 3.4 lacks `Interrupt` and
  reattach and has a different `SqlCommand`; supporting it means a
  second execution path for a version Spark no longer releases.
- **D8 (C).** Always reattachable, inline release, no other retry policy:
  a first release that silently re-runs a statement with side effects
  is worse than one that errors.
- **D9 (C).** `FetchErrorDetails` on request, not on every error.
- **D10 (C).** The type edge is `zuarrow`'s rule, unchanged; `zuspark`
  adds no conversion of its own, not even for timestamps.
- **D11 (C).** The `spark_` prefix, shared with `sparklyr`, accepted for
  familiarity; the session class differs, so no method collides.
- **D12 (C).** Unreserved URL parameters become headers, as in PySpark
  and Go, so Databricks works before Stage 5 names anything.
- **D13 (C).** No threads, no background release, no async: one channel,
  one call at a time, interrupts through cancellation.

## 14. Later versions, in the order they are expected

- **A DBI shim**: `dbConnect()`, `dbGetQuery()`, `dbSendQuery()` over
  `spark_sql()`, then `dbplyr` through `backend-spark-sql.R`.
- **Parameterised SQL** through `SQL.named_arguments` and
  `pos_arguments`, which need `Expression.Literal` built from R values,
  the first piece of a plan builder.
- **`spark_write()`**: `LocalRelation` from a `zuarrow` stream, so an R
  data frame becomes a temporary view; `AddArtifacts` for files.
- **A Databricks control-plane client**: OAuth, cluster state, start and
  wait, `connectcreds`-style credential discovery, as REST calls on the
  `zuhttp` already beneath `zugrpc`; `azr` only if its token flows are
  worth its `httr2` import.
- **`VARIANT` decoding** once `zuarrow` or a sibling has the encoding.
- **Chunked results** (`ResultChunkingOptions`), `CloneSession`,
  `AnalyzePlan.Explain` as `spark_explain()`, an opt-in retry policy.

## 15. Open questions

- **Q1.** Pin the descriptor set at `v4.3.0` while the Docker image is
  4.2.1, or both at 4.2.1 until a 4.3.0 image lands? The eight RPCs used
  are identical between the tags; this RFC says `v4.3.0`, with the image
  as the thing to bump. *2026-11-05:* zugrpc's
  `tools/gen-spark-descriptors.sh` already builds the set with `protoc`
  from the protos at `v4.2.1`, matching its 4.2.1 image; Stage 0 can
  reuse it at either tag.
  **Decided 2026-11-05: both at `v4.2.1`.** The set is pinned to the tag
  of the image the live tests run, so what is encoded and what the
  server speaks are one version; both move to 4.3.0 together, in one
  stage-less pull request, once the image exists.
- **Q2.** Should `spark_sql()` send the deprecated `SqlCommand.sql` as
  well as `input`, for 3.5 servers, or branch on `spark_version()`? Both
  encode; the snapshot shows which. **Decided 2026-11-05: send both.**
  Spark 4.2.1's `handleSqlCommand` uses `input` when it is present (read
  in `SparkConnectPlanner.scala`, and checked live), and a 3.5 server
  ignores the unknown field 6 and reads `sql`: one code path, no version
  branch. Not yet run against a 3.5 server.
- **Q3.** The Databricks header names and serverless requirements (§7.8)
  are unverified; Stage 5 answers against a workspace. **Answered
  2026-11-06** against a Free Edition workspace: serverless needs
  `x-databricks-session-id`, a cluster `x-databricks-cluster-id`, and
  `user_agent` needs nothing; see §7.8 and
  [databricks.md](databricks.md).
- **Q4.** Does `zuhttp`'s HTTP/2 streaming expose cancellation in a way
  an R interrupt can reach through `zugrpc` while `zuarrow`'s
  `get_next()` is blocked in C? If not, §7.9's interrupt path waits for
  the next batch boundary. *2026-11-05:* the gRPC stream is read in R
  through `$next_message()` before `zuarrow` decodes anything (§7.5),
  and zugrpc raises `zugrpc_error_interrupt` on Ctrl-C during a read,
  so the blocking case is a read, not `get_next()`. **Answered
  2026-11-05 (Stage 4):** yes. A real SIGINT three seconds into a
  `range(10^9)` aggregation reached the read, which zuspark catches as
  R's `interrupt` or zugrpc's `zugrpc_error_interrupt`; it cancelled the
  stream, sent `Interrupt` for the operation, and the server answered
  with that operation id, 3.1 s after the query started. The live test
  sends the signal itself (Unix only).
- **Q5.** Is a `zuhttp` HTTP/2 minor after 1.0 the right place in the
  queue for a dependency three packages deep, or should `zuspark` wait
  here until that release is dated? *2026-11-05:* the HTTP/2 code is
  already on `zuhttp` `main` (§4 update), so the question is now which
  `zuhttp` CRAN release carries it; zugrpc's roadmap records that
  0.1.0 does (its Stage 4).
- **Q6.** Is v0.1.0 a CRAN submission? `CRAN intent` records it. **Decided
  2026-11-06 by the maintainer:** 0.1.0 is a GitHub release, with its
  dependencies from GitHub through `Remotes:`; zuspark goes to CRAN in a
  later version, once the gate of §4 holds. Exact type conversion (§7.7)
  is left out of 0.1.0 the same day.

## 16. What this RFC does not decide

The print methods, whether `spark_collect()` returns a plain data frame
or a `zutable` (plain, unless a consumer asks), the release stride `k`
of §7.4, how `summary()` lays out metrics, the fields of
`spark_error_details()`'s data frames, the article's examples, and the
names of any `zugrpc` or `zuhttp` functions, which belong to RFC 0069
and to `zuhttp`'s design.

## 17. Sources and verification

Checked on 3 November 2026:

- `apache/spark` at `v4.3.0` (the newest tag, commit 2026-10-27): the
  `spark/connect/*.proto` files; `sql/connect/docs/client-connection-string.md`;
  the PySpark client's `core.py`, `session.py`, `plan.py` and
  `reattach.py`; the server's `SparkConnectPlanner.scala`
  (`handleSqlCommand`), `SparkConnectPlanExecution.scala`,
  `ErrorUtils.scala`, `Connect.scala` and `ConnectCommon.scala`;
  `ArrowConverters.scala`; `ArrowUtils.scala`; `sbin/`;
  `docs/index.md`; `R/pkg/DESCRIPTION`. `base.proto` at `v3.4.0`,
  `v3.5.0` and `v4.0.0`; `commands.proto` at `v3.4.0` and `v3.5.0`;
  `R/pkg/DESCRIPTION` at `v4.0.0` and `v4.1.0`.
- The Spark Connect overview page; the 4.0.0 SparkR page; CRAN's
  `SparkR` index page; Docker Hub's `apache/spark` tags;
  `apache/spark-docker`'s version directories; the Databricks Connect
  "cluster-config" and "python/install" pages.
- `mlverse/pysparklyr`'s `DESCRIPTION` and `R/connect-databricks*.R`;
  `apache/spark-connect-go`'s `channel.go`; `apache/spark-connect-rust`'s
  README and metadata; `tidyverse/dbplyr`'s `R/backend-spark-sql.R`.
- crandb for every package named in §1.2, §3 and §4; `pedrobtz/zuproto`,
  `zuarrow` and `zuid`: `DESCRIPTION`, stage issues, open issues and
  design documents (zuproto §2, §3, §5, §10; zuarrow §6.2, §6.3, §6.7,
  §14; zuid §5); `pedrobtz/zuhttp`'s `DESCRIPTION` and issues #22 and
  #40. The `zugrpc`-on-`zuhttp` split is the maintainer's decision of
  2026-11-03, relayed while drafting, and in no repository yet.

Not verified, left to the stage named: the Databricks header names and
serverless requirements, and what `databricks-connect` sends as
`user_agent` (Stage 5; answered 2026-11-06 in §7.8, against a workspace
rather than by reading `databricks-connect`); `TimeType`'s Arrow form beyond "through
`TypeApiOps` with precision in field metadata" (Stage 3); the exact
`INVALID_HANDLE.OPERATION_NOT_FOUND` string and retry conditions in
`reattach.py`, read from its docstring only (Stage 2); the default of
`spark.connect.grpc.arrow.maxBatchSize` (Stage 2); whether Spark 3.5
answers a `SqlCommand` that sets only `input` (Stage 2, Q2); which
`duckdb` entry point takes a `zuarrow` stream (answered 2026-11-04 in
§7.6: none without `arrow`); the
container's start-up time in CI (Stage 1).
