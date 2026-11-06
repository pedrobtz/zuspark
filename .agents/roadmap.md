# zuspark: roadmap

Status: draft 2026-11-05. Implements [design.md](design.md).
Target: v0.1.0, the first CRAN release.
Tracking: [v0.1.0 (#1)](https://github.com/pedrobtz/zuspark/issues/1), with one
sub-issue per stage;
[CRAN-submission (#13)](https://github.com/pedrobtz/zuspark/issues/13).

The stages follow the design's own plan, moved here from its §11. Stages 0
to 8 keep the design's numbers, so the design's references to them still
hold. Stage 8 (Full CI) is followed by Stages 9 (v0.1.0 readiness) and 10
(Review). One change from the design: the live-Spark CI job moves from
Stage 6 to Stage 1, because zugrpc's `live.yaml` already runs Spark 4.2.1
on Ubuntu (design §9), so every stage from 1 on is checked against a real
server. Stage 6 keeps the full live suite, the hostile responses and the
benchmarks.

## Stages

<!-- markdownlint-disable MD013 -->

| Stage | Title | CI | Issue | Pull request | Done |
| --- | --- | --- | --- | --- | --- |
| 0 | [Package identity and descriptor set](#stage-0--package-identity-and-descriptor-set) | quick | [#2](https://github.com/pedrobtz/zuspark/issues/2) | [#15](https://github.com/pedrobtz/zuspark/pull/15) | 2026-11-05 |
| 1 | [Connection and session](#stage-1--connection-and-session) | quick | [#3](https://github.com/pedrobtz/zuspark/issues/3) | [#16](https://github.com/pedrobtz/zuspark/pull/16) | 2026-11-05 |
| 2 | [SQL execution and results](#stage-2--sql-execution-and-results) | full | [#4](https://github.com/pedrobtz/zuspark/issues/4) | [#17](https://github.com/pedrobtz/zuspark/pull/17) | 2026-11-05 |
| 3 | [Schema and types](#stage-3--schema-and-types) | quick | [#5](https://github.com/pedrobtz/zuspark/issues/5) | [#18](https://github.com/pedrobtz/zuspark/pull/18) | 2026-11-05 |
| 4 | [Errors, interrupts and deadlines](#stage-4--errors-interrupts-and-deadlines) | quick | [#6](https://github.com/pedrobtz/zuspark/issues/6) | [#19](https://github.com/pedrobtz/zuspark/pull/19) | 2026-11-05 |
| 5 | [Databricks](#stage-5--databricks) | quick | [#7](https://github.com/pedrobtz/zuspark/issues/7) | | 2026-11-06 |
| 6 | [Live suite, hostile responses and benchmarks](#stage-6--live-suite-hostile-responses-and-benchmarks) | full | [#8](https://github.com/pedrobtz/zuspark/issues/8) | | |
| 7 | [Documentation](#stage-7--documentation) | quick | [#9](https://github.com/pedrobtz/zuspark/issues/9) | | |
| 8 | [Full CI](#stage-8--full-ci) | full | [#10](https://github.com/pedrobtz/zuspark/issues/10) | | |
| 9 | [v0.1.0 readiness](#stage-9--v010-readiness) | full | [#11](https://github.com/pedrobtz/zuspark/issues/11) | | |
| 10 | [Review](#stage-10--review) | full | [#12](https://github.com/pedrobtz/zuspark/issues/12) | | |

<!-- markdownlint-enable MD013 -->

The live-Spark workflow (`live.yaml`, Ubuntu only, the one runner with
Docker) runs on every pull request from Stage 1 on, beside the quick
profile. Its time is its own; the five-minute target is the quick
profile's.

## Stage 0 — Package identity and descriptor set

**Issue.** [#2](https://github.com/pedrobtz/zuspark/issues/2), sub-issue of
[v0.1.0](https://github.com/pedrobtz/zuspark/issues/1).

**CI.** `quick`.

**Goal.** The empty package has its real identity and CI, no native code,
and ships the Spark Connect descriptor set that every later stage encodes
against.

**Work.**

- `DESCRIPTION`: real Title and Description, `Authors@R` with `cre`, `aut`
  and `cph`, `Depends: R (>= 4.1)`, `Language: en-GB`, MIT licence for
  zuspark's own code, `Version: 0.0.0.9000`. `Imports: zuproto` with its
  `Remotes:`; the other `Imports` arrive with the stage that uses them.
- The `create-pkg.sh` C stub removed: `src/` and the `useDynLib`
  directive go (D3); `tests/testthat/test-abi.R` asserts `src/` is absent.
- `R-CMD-check` caller with the `{"os": "windows-latest", "r": "devel"}`
  runner row.
- `tools/build-descriptors.sh`, started from zugrpc's
  `tools/gen-spark-descriptors.sh`: `protoc --descriptor_set_out` over
  the pinned Spark protos plus `google/rpc/{status,error_details}.proto`.
  The set in `inst/proto/`, the tag and SHA-256 in `inst/proto/VERSION`,
  Spark's Apache-2.0 licence and `NOTICE` beside it, and the bundled
  copyright in `DESCRIPTION` and `LICENSE` notes.
- `CLAUDE.md` (written by `pkg-init`, 2026-11-05).

**Exit criteria.**

- `R CMD check` passes on the quick r-actions profile with 0 errors, 0
  warnings and 0 notes.
- Every check of the quick profile on the pull request finishes in under
  five minutes; what was tuned, if anything, is recorded here.
- The `R-CMD-check` caller lists the Windows R-devel runner.
- There is no `src/`, and `test-abi.R` passes.
- `proto_pool()` loads the shipped set, and `proto_types()` lists
  `spark.connect.ExecutePlanRequest` and `google.rpc.ErrorInfo`.
- `inst/proto/VERSION` records the Spark tag and the set's SHA-256, and a
  test checks the hash.

**Design.** [§3](design.md#3-position-in-the-zu-family),
[§5](design.md#5-the-protocol-as-zuspark-uses-it), D3 and D4 of
[§13](design.md#13-decisions).

## Stage 1 — Connection and session

**Issue.** [#3](https://github.com/pedrobtz/zuspark/issues/3), sub-issue of
[v0.1.0](https://github.com/pedrobtz/zuspark/issues/1).

**CI.** `quick`.

**Goal.** `spark_connect()` opens a session on a real server, proves it with
`Config` and `SparkVersion`, refuses servers below 3.5.0, and
`spark_disconnect()` releases it; the same exchange replays from recorded
fixtures on every leg.

**Work.**

- The §7.1 connection-string parser, environment-variable defaults,
  `headers` and `...` parameters, `zuspark_url_error`.
- The channel through `zugrpc` (added to `Imports` and `Remotes:`), session
  ids from `zuid`, `client_type`, the server-side session id echo.
- `spark_connect()`, `spark_disconnect()` with `ReleaseSession` and its
  finalizer, `spark_version()`, `spark_config()`, `spark_session_id()`,
  `zuspark_info()`, the version floor and `zuspark_version_error`.
- The fake channel and the fixture layout of §9, recorded from the
  container.
- `.github/workflows/live.yaml`, from zugrpc's `spark` job, running the
  live tests against `apache/spark:4.2.1` on Ubuntu.

**Exit criteria.**

- PySpark's four connection-string errors are reproduced as
  `zuspark_url_error`.
- Connect, config get and set, version and disconnect pass on the recorded
  fixtures on every quick-profile leg.
- The same tests pass against live Spark in `live.yaml` on the pull request.
- A server reporting a version below 3.5.0, in a fixture, gives
  `zuspark_version_error`.

**Design.** [§7.1](design.md#71-connection-strings),
[§7.2](design.md#72-sessions), [§9](design.md#9-testing).

## Stage 2 — SQL execution and results

**Issue.** [#4](https://github.com/pedrobtz/zuspark/issues/4), sub-issue of
[v0.1.0](https://github.com/pedrobtz/zuspark/issues/1).

**CI.** `full — first server-supplied Arrow and protobuf payloads decoded
through zuarrow's and zuproto's C under the reattach loop every later stage
builds on: ASan and valgrind.`

**Goal.** SQL runs: commands return their rows at once, queries return a
`zuspark_query` that `spark_collect()` and `spark_fetch()` execute, and a
stream the server cuts is reattached with every row arriving once.

**Work.**

- `ExecutePlan` with `SqlCommand` over a `SQL` relation, both forms of
  `SqlCommandResult`, the `ArrowBatch` stream into `zuarrow`,
  `ResultComplete` (`zuarrow` added to `Imports` and `Remotes:`).
- Reattachable execution: `operation_id`, `ReattachExecute`,
  `ReleaseExecute` every `k` responses and on completion; `k` chosen and
  recorded in the design.
- `spark_sql()`, `spark_collect()`, `spark_fetch()`, `spark_execute()`,
  `as.data.frame()` on a query; `row_count` and `start_offset` checks.
- Proto snapshots of every encoded `Plan`.

**Exit criteria.**

- `range(10^8)` is collected once through a cut stream (server
  `senderMaxStreamDuration` set low) in `live.yaml`, every row exactly once.
- The two `SqlCommandResult` forms pass on recorded fixtures on every leg.
- Every SQL call's encoded `Plan` has a hex and JSON snapshot.
- A full-profile run is green on the pull request.

**Design.** [§6](design.md#6-public-r-api-v010-shape),
[§7.3](design.md#73-executing-sql-the-two-step-protocol),
[§7.4](design.md#74-reattachable-execution),
[§7.5](design.md#75-result-batches).

## Stage 3 — Schema and types

**Issue.** [#5](https://github.com/pedrobtz/zuspark/issues/5), sub-issue of
[v0.1.0](https://github.com/pedrobtz/zuspark/issues/1).

**CI.** `quick`.

**Goal.** Every Spark SQL type of §7.7 arrives at the R edge exactly as
PySpark's bytes say, or errors as `lossy` says.

**Work.**

- `AnalyzePlan.Schema` and `spark_schema()`.
- The `lossy` and `to` pass-through to `zuarrow`.
- The type matrix: PySpark writes one IPC payload per row of §7.7's table
  in the container; fixtures of those bytes for the other legs.
- `TIME(p)` decided and recorded in the design.
- `zuint`, `decimal`, `zutempo` in `Suggests`, with guards.

**Exit criteria.**

- §7.7's table passes against PySpark's IPC bytes, with and without
  `zuint`, `decimal` and `zutempo` loaded.
- `spark_schema()` on a query matches the schema of its collected result.

**Deviation (2026-11-05, agreed with the maintainer).** None of `zuint`,
`decimal` and `zutempo` registers its exact targets with `zuarrow` yet
([zuint#52](https://github.com/pedrobtz/zuint/issues/52),
[decimal#24](https://github.com/pedrobtz/decimal/issues/24),
[zutempo#94](https://github.com/pedrobtz/zutempo/issues/94)), so the
first exit criterion holds without them loaded only. The checks with them
loaded are in `test-types.R` and skip until each package registers; they
run with no change here once one does. PySpark's `toArrow()` refuses the
two interval types without a `duration` form, so those are checked
against the server alone. `spark_schema()` returns Spark's schema, not an
Arrow one (design §6).

**Design.** [§7.7](design.md#77-types).

## Stage 4 — Errors, interrupts and deadlines

**Issue.** [#6](https://github.com/pedrobtz/zuspark/issues/6), sub-issue of
[v0.1.0](https://github.com/pedrobtz/zuspark/issues/1).

**CI.** `quick`.

**Goal.** Every failure is one of §8's classes with Spark's details on it,
and a query can be stopped on the server.

**Work.**

- §8's condition classes from zugrpc's conditions and `ErrorInfo`;
  `spark_error_details()` over `FetchErrorDetails`.
- `spark_interrupt()`, the interrupt path of §7.9 on an R interrupt, and
  deadlines.

**Exit criteria.**

- A `SELECT` with a typo yields `zuspark_sql_error` with error class,
  SQLSTATE and parameters; a missing table gives `TABLE_OR_VIEW_NOT_FOUND`
  and `42P01`, live and from fixtures.
- An interrupt during `range(10^9)` stops the server job, and the
  `interrupted_ids` are reported; Q4's answer is recorded in the design.
- Every class of §8's table has a test.

**Design.** [§7.9](design.md#79-interrupts-and-deadlines),
[§8](design.md#8-errors).

## Stage 5 — Databricks

**Issue.** [#7](https://github.com/pedrobtz/zuspark/issues/7), sub-issue of
[v0.1.0](https://github.com/pedrobtz/zuspark/issues/1).

**CI.** `quick`.

**Goal.** zuspark is known to work, or known not to and why, against a
Databricks workspace.

**Work.**

- §7.8's questions answered against a workspace: header names, serverless,
  `user_agent`.
- Header plumbing fixes they need; `spark_connect_databricks()` if
  warranted.

**Exit criteria.**

- The answers are recorded in `.agents/databricks.md` and Q3 is closed in
  the design.
- A workspace query is recorded as a fixture with secrets redacted, and
  replays on every leg.

**Design.** [§7.8](design.md#78-databricks-connect).

## Stage 6 — Live suite, hostile responses and benchmarks

**Issue.** [#8](https://github.com/pedrobtz/zuspark/issues/8), sub-issue of
[v0.1.0](https://github.com/pedrobtz/zuspark/issues/1).

**CI.** `full — the hostile-response suite feeds truncated and corrupted
server payloads to the decoders, which belongs on the sanitizer legs.`

**Goal.** Every behaviour of §7 and §8 is tested live, no hostile response
crashes, and §10's targets are measured.

**Work.**

- The live suite completed to cover §7 and §8; the reattach test of §9.
- Hostile responses: truncations, byte flips, oversized counts.
- `tools/run-benchmarks` against PySpark in the container; results in
  `.agents/benchmarks.md`.

**Exit criteria.**

- §9's live suite is green in `live.yaml`.
- Every hostile response gives `zuspark_protocol_error`, never a crash,
  on a full-profile run.
- §10's table is recorded, with each target met or its gap explained.

**Design.** [§9](design.md#9-testing),
[§10](design.md#10-performance-targets).

## Stage 7 — Documentation

**Issue.** [#9](https://github.com/pedrobtz/zuspark/issues/9), sub-issue of
[v0.1.0](https://github.com/pedrobtz/zuspark/issues/1).

**CI.** `quick`.

**Goal.** A user can learn the package from its site.

**Work.**

- README, the reference index, examples guarded for when no server is
  reachable.
- The Get started article run for real (`eval` on) and the article
  "Spark without the JVM" with the DuckDB hand-off as its centrepiece.

**Exit criteria.**

- Every example and article chunk runs against the 4.2.1 container.
- `R CMD check` passes on the quick profile with no server reachable.
- The pkgdown site builds with both articles in the navbar.

**Design.** [§6](design.md#6-public-r-api-v010-shape),
[§7.6](design.md#76-the-duckdb-hand-off).

## Stage 8 — Full CI

**Issue.** [#10](https://github.com/pedrobtz/zuspark/issues/10), sub-issue of
[v0.1.0](https://github.com/pedrobtz/zuspark/issues/1).

**CI.** `full`, always.

**Goal.** The full r-actions profile is green on the code Stages 0 to 7
merged. No new features.

**Work.**

- Run the full profile (`full-ci` label) and fix what it finds: every
  platform leg, the containers, `nosuggests`, `nold` and any failures
  recorded on this stage's issue.

**Exit criteria.**

- A full-profile run is green on the pull request and on its merge commit.

**Design.** [§12](design.md#12-acceptance-criteria-for-v010).

## Stage 9 — v0.1.0 readiness

**Issue.** [#11](https://github.com/pedrobtz/zuspark/issues/11), sub-issue of
[v0.1.0](https://github.com/pedrobtz/zuspark/issues/1).

**CI.** `full`, always.

**Goal.** The package is a release candidate that installs from CRAN
dependencies only.

**Work.**

- `Version: 0.1.0` and the matching `NEWS.md` heading.
- `Remotes:` removed, with `Imports` floors on the CRAN versions of
  `zugrpc`, `zuproto`, `zuarrow` and `zuid`.
- `cran-comments.md` listing the r-actions CI legs as test environments.
- `urlchecker::url_check()` and `spelling::spell_check_package()` clean.

**Exit criteria.**

- `DESCRIPTION` has no `Remotes:`, and its floors are versions on CRAN.
- A full-profile run is green on the merge commit.

**Design.** [§4](design.md#4-dependencies-and-the-release-gate),
[§12](design.md#12-acceptance-criteria-for-v010).

## Stage 10 — Review

**Issue.** [#12](https://github.com/pedrobtz/zuspark/issues/12), sub-issue of
[v0.1.0](https://github.com/pedrobtz/zuspark/issues/1).

**CI.** `full`, always.

**Goal.** The release candidate is reviewed and every blocking finding is
fixed. No new features. Only submission is left when it closes.

**Work.**

1. `pkg-critical-review` (code, tests and style). Blocking and required
   findings become sub-issues of this stage's issue, each fixed in a pull
   request that closes it.
2. `pkg-cran-review`, after those fixes, with every FAIL and WARN resolved.

**Exit criteria.**

- Every blocking and required review issue is closed.
- The `pkg-cran-review` table is clean.
- `NEWS.md` reflects any user-visible fix.
- A full-profile run is green on the default branch, whose head becomes the
  release commit.

**Design.** [§12](design.md#12-acceptance-criteria-for-v010).

## After 0.1.0

- [§14](design.md#14-later-versions-in-the-order-they-are-expected): the DBI
  shim and dbplyr, parameterised SQL, `spark_write()`, a Databricks
  control-plane client, `VARIANT` decoding, chunked results, `CloneSession`
  and `spark_explain()`. Each is beyond SQL-in, Arrow-out (D2, D5);
  `spark_write()` also needs client streaming in zugrpc.

## Blocking open questions

- [§15](design.md#15-open-questions) Q5 and Q6 block Stage 9: when the
  siblings reach CRAN, and whether 0.1.0 is a CRAN submission at all.
  zugrpc's roadmap records that zuhttp 0.1.0 carries HTTP/2.
