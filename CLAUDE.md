# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working
with code in this repository.

## Shared conventions

The portfolio’s shared rules are synced into `.claude/shared/` from
`pedrobtz/packages:claude/shared/`. Never edit them here. Where this
file and a shared file disagree, this file wins for this package.

@.claude/shared/workflow.md @.claude/shared/r-package.md
@.claude/shared/ci.md

## What this is

`zuspark` is a Spark Connect client for R with no JVM, no Python and no
Spark installation. It connects to an `sc://` endpoint, runs SQL there
and returns the result as Arrow, speaking the protocol itself: gRPC
through `zugrpc`, protobuf through `zuproto`, Arrow IPC through
`zuarrow`, ids from `zuid`. It is an application package, pure R over
those four `Imports`. v0.1.0 covers SQL only: connect, run a statement,
read a schema, collect or stream rows, interrupt, disconnect. It is
deliberately not a DBI driver, a dplyr backend, a plan builder, a UDF
runner, a local Spark or an ML or streaming client; the DBI shim and the
rest are later versions.

The specification is
[`.agents/design.md`](https://pedrobtz.github.io/zuspark/.agents/design.md),
adopted from RFC 0070. It outranks this file: `CLAUDE.md` orients, the
design decides. Its decisions (§13) and open questions (§15) are
recorded there, not here.

## Current state

Scaffolded 2026-11-05 with `create-pkg.sh`; no functionality yet. The
roadmap in `.agents/roadmap.md` takes it to v0.1.0. Release is gated on
five siblings reaching CRAN (design §4). None is on CRAN yet; `zugrpc`
(verified 2026-11-05 at `ba9d83b`) already works against live Spark on
`zuhttp`’s and `zuproto`’s `main`, and its own release waits on
zuhttp#40 and zuproto#13. Its R API can still change before then.

## Versions and dependents

The current version’s parent issue is the `v0.1.0` issue, created by
`pkg-roadmap`. Sibling packages that depend on this one before CRAN
does: none.

## Commands

The siblings come from GitHub until they reach CRAN:
`pak::pak("pedrobtz/zugrpc")` pulls `zuhttp` and `zuproto` through its
`Remotes:`. Stage 0 adds `tools/build-descriptors.sh`, which runs
`protoc` over the pinned Spark protos (design §11, D4), starting from
zugrpc’s `tools/gen-spark-descriptors.sh`; it is occasional, run only
when the pinned Spark tag changes.

## Architecture

To be written as stages land; see `.agents/design.md`. The scaffold set
up a C layer (`src/zuspark.c`, `useDynLib` in `R/zuspark-package.R`)
that the design does not want; see the first invariant.

## Invariants that are easy to break

- **No `src/`** (D3). `zuspark` is pure R; native code lives in `zuhttp`
  and `zuarrow`. The `create-pkg.sh` C stub and the `useDynLib`
  directive are to be removed in Stage 0, and `test-abi.R` then asserts
  `src/` stays absent.
- **The descriptor set is built by a `tools/` script, never at install
  time** (D4). It ships compiled in `inst/`, pinned to one Spark tag
  with its SHA-256 recorded; re-pinning re-runs the proto snapshots.
- **No type conversion of `zuspark`’s own** (D10). Every row goes to
  `zuarrow` untouched, timestamps included; the R edge is `zuarrow`’s
  `lossy` rule.
- **No threads, no background work, no retries beyond reattach** (D8,
  D13). One channel, one call at a time; a release that silently re-runs
  a statement with side effects is worse than an error.

## Testing

The conformance oracle is PySpark: recorded request and response
fixtures replayed through a fake channel on every leg, live Spark
Connect in a container on the Ubuntu legs only (design §9), since only
Ubuntu runners have Docker. None of it exists yet; the live job will
start from zugrpc’s `live.yaml` recipe (design §9). A gap found in
`zugrpc` while testing is filed there as a zugrpc issue, not worked
around here. Add the helpers and the suite’s runtime here as stages
land. The conventions every package follows are in
`.claude/shared/r-package.md`.

## Continuous integration

`R-CMD-check.yaml` and `coverage.yaml` call the shared
`pedrobtz/r-actions` workflows at `@v1`; `pkgdown.yaml` builds and
deploys the site with `r-lib/actions`. The stages marked `full` are
listed in `.agents/roadmap.md`. What a change runs and what the CI
labels do is in `.claude/shared/ci.md`.
