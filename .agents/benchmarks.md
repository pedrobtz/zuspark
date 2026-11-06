# zuspark benchmarks

Design §10's targets, measured by `tools/run-benchmarks.R` (zuspark) and
`tools/benchmarks.py` (PySpark), against the same server from the same
machine:

```sh
Rscript tools/run-benchmarks.R sc://localhost:15002 <python with pyspark-client>
```

## 2026-11-06

macOS arm64, R 4.5.2, zuspark at the Stage 6 branch, zugrpc, zuhttp and
zuarrow at `main`; PySpark 4.2.1 (`pyspark-client`) with pyarrow 26.0.1;
Spark Connect 4.2.1 in Podman with `--driver-memory 3g`, reached through
Podman's port forward. Medians: 5 connects, 50 round trips, 3 collects.
The query is `SELECT id, id * 2 AS a, CAST(id AS DOUBLE) AS b, id % 7 AS
c FROM range(10000000)`: about 326 MB of Arrow in about 1000 batches.

| Operation | Target | zuspark | PySpark | Ratio | Met |
| --- | --- | --- | --- | --- | --- |
| `spark_connect()` | under 100 ms | 65 ms | 103 ms | 0.63× | yes |
| `SELECT 1`, round trip | within 2× | 114 ms | 123 ms | 0.92× | yes |
| 10⁷ × 4, `spark_collect()` vs `toArrow()` | within 1.5× | 16.1 s | 9.0 s | 1.78× | no |
| 10⁷ × 4, `spark_collect()` vs `toPandas()` | (context) | 16.1 s | 21.1 s | 0.76× | |
| the same into DuckDB through `arrow` | no R-heap copy | 492 MB on the R heap | | | no |
| R heap above the result, `spark_collect()` | under one batch (4 MiB) | 376 MB | | | no |

### The gaps

- **Throughput (1.78× of `toArrow()`).** Profiled on 10⁶ rows, nearly
  all of the time is in reading the stream: zugrpc's `next_message()`,
  in zuhttp's `zu_resp_read()` and zugrpc's R-level `frame_split()`;
  protobuf decoding is about 8% and zuarrow's conversion less. The
  transport is zugrpc's and zuhttp's to profile (design §10), filed as
  [zugrpc#55](https://github.com/pedrobtz/zugrpc/issues/55). zuspark
  already beats PySpark's `toPandas()`, the closer analogue of a data
  frame.
- **Memory (DuckDB and `spark_collect()`).** Both are §7.5's design, not a
  bug: `spark_fetch()` gathers every batch's IPC bytes as raw vectors and
  hands them to `arrow_read(lazy = TRUE)` in one call, so the whole
  result's Arrow bytes (326 MB here) are on the R heap before any
  conversion, and DuckDB reads them through `arrow` from there. Meeting
  "under one batch" needs a stream that decodes each batch as it arrives,
  which is §14's chunked results, after 0.1.0.
- **First query.** The first execution in a fresh server takes about
  3 s for `SELECT 1` (PySpark: 0.8 s) while Spark warms up; the numbers
  above are after a warm-up call on each side.
- Podman's port forward adds variance: the same 10⁶-row read took 1.4 to
  4.2 s across runs.
