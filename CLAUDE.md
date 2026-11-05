# CLAUDE.md

<!-- markdownlint-disable-next-line MD013 -->
This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Shared conventions

The portfolio's shared rules are synced into `.claude/shared/` from
`pedrobtz/packages:claude/shared/`. Never edit them here. Where this file and
a shared file disagree, this file wins for this package.

@.claude/shared/workflow.md
@.claude/shared/r-package.md
@.claude/shared/ci.md

## What this is

`zuspark` is a Spark Connect client for R with no JVM, no Python and no Spark
installation. It connects to an `sc://` endpoint, runs SQL there and returns
the result as Arrow, speaking the protocol itself: gRPC through `zugrpc`,
protobuf through `zuproto`, Arrow IPC through `zuarrow`, ids from `zuid`. It
is an application package, pure R over those four `Imports`. v0.1.0 covers
SQL only: connect, run a statement, read a schema, collect or stream rows,
interrupt, disconnect. It is deliberately not a DBI driver, a dplyr backend,
a plan builder, a UDF runner, a local Spark or an ML or streaming client;
the DBI shim and the rest are later versions.

The specification is [`.agents/design.md`](.agents/design.md), adopted from
RFC 0070. It outranks this file: `CLAUDE.md` orients, the design decides.
Its decisions (§13) and open questions (§15) are recorded there, not here.

## Current state

Stages 0 to 3 done 2026-11-05: the descriptor set pinned at Spark
`v4.2.1`, and sessions: `spark_connect()`, `spark_disconnect()`,
`spark_config()`, `spark_version()`, `spark_session_id()`,
`zuspark_info()`, and SQL with reattachable execution: `spark_sql()`,
`spark_collect()`, `spark_fetch()`, `spark_execute()` (Stage 2), and
`spark_schema()` (Stage 3). Exact conversion of `int64`, `decimal128`
and the time types waits on zuint, decimal and zutempo registering with
zuarrow (zuint#52, decimal#24, zutempo#94). The
roadmap in
`.agents/roadmap.md` takes it to v0.1.0. Release is gated on five
siblings reaching CRAN (design §4). None is on CRAN yet; `zugrpc`
(verified 2026-11-05 at `ba9d83b`) already works against live Spark on
`zuhttp`'s and `zuproto`'s `main`, and its own release waits on zuhttp#40
and zuproto#13. Its R API can still change before then.

## Versions and dependents

The current version's parent issue is the `v0.1.0` issue, created by
`pkg-roadmap`. Sibling packages that depend on this one before CRAN does:
none.

## Commands

The siblings come from GitHub until they reach CRAN, through `Remotes:`;
`pak::pak("pedrobtz/zugrpc")` also pulls `zuhttp` and `zuproto`.

The live tests need a Spark Connect 4.2.1 server; without Docker,
Podman runs the same image:

```sh
podman run -d --name spark -p 15002:15002 -e SPARK_NO_DAEMONIZE=true \
  docker.io/apache/spark:4.2.1 /opt/spark/sbin/start-connect-server.sh \
  --conf spark.connect.grpc.binding.address=0.0.0.0
ZUSPARK_TEST_REMOTE=sc://localhost:15002 Rscript -e 'devtools::test()'
Rscript tools/record-fixtures.R sc://localhost:15002   # re-record fixtures
```

The reattach test needs a second server that cuts streams, with a heap
large enough to buffer `range(10^8)`; the `reattach` fixture is recorded
from one that cuts after 2 KB (`tools/record-fixtures.R` says how):

```sh
podman run -d --name spark-cut -p 15003:15002 -e SPARK_NO_DAEMONIZE=true \
  docker.io/apache/spark:4.2.1 /opt/spark/sbin/start-connect-server.sh \
  --driver-memory 3g --conf spark.connect.grpc.binding.address=0.0.0.0 \
  --conf spark.connect.execute.reattachable.senderMaxStreamDuration=1s \
  --conf spark.connect.execute.reattachable.senderMaxStreamSize=1m
ZUSPARK_TEST_REMOTE_CUT=sc://localhost:15003 \
  Rscript -e 'devtools::test(filter = "live")'
```

Re-record the fixtures when a stage adds an RPC scenario or the request
shape changes, and commit them with the change.

`sh tools/build-descriptors.sh` rebuilds `inst/proto/spark-connect.binpb`
and `inst/proto/VERSION` (needs `curl` and `protoc`). It is occasional:
run it only when a pin at its top changes, and commit the set, `VERSION`
and `NOTICE` together; `test-proto.R` checks the hash.

## Architecture

See `.agents/design.md` for the why; this is the where.

- [R/proto.R](R/proto.R) loads the shipped descriptor set into one zuproto
  pool per R session (`spark_pool()`).
- [R/transport.R](R/transport.R): every RPC goes through `spark_rpc()`,
  which encodes, calls `con$transport` and decodes. The transport is
  bound when the connection is made: normally a unary `zugrpc` call,
  in tests the fake channel from the `zuspark.transport` option. The
  `zuspark.record` option writes each call's bytes for the fixtures.
- [R/execute.R](R/execute.R): `execute_plan()` runs one ExecutePlan
  with reattachable execution: the reattach loop, inline releases every
  16 responses, release-all on completion or abandonment, and the
  per-response checks. [R/sql.R](R/sql.R) builds the user verbs on it.
- [R/url.R](R/url.R) parses `sc://` strings; [R/session.R](R/session.R)
  holds the connection, an environment of class `zuspark_connection`, so
  the server-side id and the released flag are shared by every copy;
  [R/config.R](R/config.R) is `spark_config()`.
- [R/conditions.R](R/conditions.R): `zuspark_abort()` raises every
  `zuspark_error`, base R conditions only.

Licences of the bundled protocol files are in
[inst/COPYRIGHTS](inst/COPYRIGHTS).

## Invariants that are easy to break

- **No `src/`** (D3). `zuspark` is pure R; native code lives in `zuhttp`
  and `zuarrow`. `test-abi.R` fails if a shared library is ever built or
  loaded.
- **The descriptor set is built by a `tools/` script, never at install
  time** (D4). It ships compiled in `inst/proto/`, pinned to the Spark tag
  of the live-test image with its SHA-256 recorded; the two are re-pinned
  together, and re-pinning re-runs the proto snapshots.
- **Every request goes through `session_fields()`**, which carries the
  session id, user context and client type, and refuses a released
  connection; every response goes through `observe_session()`. A new RPC
  that skips either loses the session checks of design §7.2.
- **A new execution path goes through `execute_plan()`.** It is what
  releases the server's buffer; an ExecutePlan that bypasses it leaves
  the result in the server's memory until the session dies.
- **The transport is read from `con$transport`, never from the option at
  call time.** A finalizer runs after the test that set the option has
  gone; binding at connect is what keeps it off the network.
- **No type conversion of `zuspark`'s own** (D10). Every row goes to
  `zuarrow` untouched, timestamps included; the R edge is `zuarrow`'s
  `lossy` rule.
- **No threads, no background work, no retries beyond reattach** (D8,
  D13). One channel, one call at a time; a release that silently re-runs a
  statement with side effects is worse than an error.

## Testing

The conformance oracle is PySpark and the live server: recorded request
and response fixtures replayed through a fake channel on every leg, live
Spark Connect in a container on the Ubuntu legs only (design §9), since
only Ubuntu runners have Docker.

- [helper-fixtures.R](tests/testthat/helper-fixtures.R): `local_replay()`
  replays a scenario of `tests/testthat/fixtures/` in order, failing on a
  call out of order, and collects the decoded requests; `responses`
  overrides a recorded answer by call number (bytes, or a function that
  may signal), which is how version, session and status cases are made
  without recording them. `replay_connect()` connects with the scenario's
  recorded session id, and mocks `new_operation_id()` so that each
  execution gets the operation id it was recorded with. Streaming calls
  replay message by message; `edit_response()` and `which_response()`
  rewrite one recorded message, and a condition in the message list is
  signalled when the stream reaches it.
- [helper-types.R](tests/testthat/helper-types.R): the type matrix,
  `fixtures/types/types.csv`, one query per Spark type, and PySpark's
  Arrow bytes for each, recorded by `tools/pyspark-types.py` inside the
  Spark container (it needs `pip install pyarrow pandas grpcio
  grpcio-status googleapis-common-protos zstandard` there first).
  Re-record them with the fixtures when the Spark pin moves.
- [helper-live.R](tests/testthat/helper-live.R): `test-live-*.R` run only
  with `ZUSPARK_TEST_REMOTE` set, and the reattach test only with
  `ZUSPARK_TEST_REMOTE_CUT`, a server that cuts streams (see Commands);
  `tools/ci-live-tests.R` runs them in `live.yaml` and fails if one
  skips. The fixture suite takes about ten seconds; the live
  `range(10^8)` reattach test about a minute.

A gap found in `zugrpc` while testing is filed there as a zugrpc issue,
not worked around here. Add the helpers and
the suite's runtime here as stages land. The conventions
every package follows are in `.claude/shared/r-package.md`.

## Continuous integration

`R-CMD-check.yaml` and `coverage.yaml` call the shared `pedrobtz/r-actions`
workflows at `@v1`; `pkgdown.yaml` builds and deploys the site with
`r-lib/actions`. `live.yaml` is this package's own: it starts Spark Connect
4.2.1 in Docker on Ubuntu, plus a second server that cuts streams for the
reattach test, and runs the live tests, because R CMD check never touches
a network. Stage 2 is the first `full` stage; the stages marked `full`
are listed in `.agents/roadmap.md`.
What a change runs and what the CI labels do is in `.claude/shared/ci.md`.
