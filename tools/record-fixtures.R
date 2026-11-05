# Records the RPC fixtures the tests replay on every leg (design §9).
#
# Run from the package root against Spark Connect 4.2.1, the image the live
# tests use:
#
#   docker run -d --name spark -p 15002:15002 -e SPARK_NO_DAEMONIZE=true \
#     apache/spark:4.2.1 /opt/spark/sbin/start-connect-server.sh \
#     --conf spark.connect.grpc.binding.address=0.0.0.0
#   Rscript tools/record-fixtures.R sc://localhost:15002 sc://localhost:15004
#
# The second server cuts every stream after 2 KB, so that the `reattach`
# scenario reattaches several times on a small result:
#
#   docker run -d --name spark-cut -p 15004:15002 -e SPARK_NO_DAEMONIZE=true \
#     apache/spark:4.2.1 /opt/spark/sbin/start-connect-server.sh \
#     --conf spark.connect.grpc.binding.address=0.0.0.0 \
#     --conf spark.connect.execute.reattachable.senderMaxStreamSize=2k
#
# Each scenario is a directory of tests/testthat/fixtures/ holding, per call
# in order, <nn>-<method>.request.binpb and .response.binpb: the encoded
# request zuspark sent and the response message's bytes as received. Each
# scenario gets a new session id, since a server refuses an id it has
# released (INVALID_HANDLE.SESSION_CLOSED); the tests read it back from the
# first recorded request. The user id is fixed; client_type carries the
# recording machine's versions.

pkgload::load_all(quiet = TRUE)

args <- commandArgs(trailingOnly = TRUE)
remote <- if (length(args) >= 1L) args[[1]] else "sc://localhost:15002"
cutting <- if (length(args) >= 2L) args[[2]] else "sc://localhost:15004"
fixtures <- file.path("tests", "testthat", "fixtures")

user_id <- "zuspark"

record <- function(scenario, code) {
  dir <- file.path(fixtures, scenario)
  unlink(dir, recursive = TRUE)
  withr::with_options(list(zuspark.record = dir), code)
  cat(sprintf("%s: %d calls\n", scenario, length(list.files(dir, "\\.request\\.binpb$"))))
}

# Connect (Config get_all, SparkVersion), read, set and unset settings,
# disconnect (ReleaseSession).
record("session", {
  con <- spark_connect(remote, user_id = user_id)
  spark_config(con, "spark.sql.session.timeZone", "zuspark.no.such.key")
  old <- spark_config(con, spark.sql.session.timeZone = "UTC", zuspark.test.key = NULL)
  spark_disconnect(con)
})

# Connect and disconnect only.
record("connect", {
  con <- spark_connect(remote, user_id = user_id)
  spark_disconnect(con)
})

# A query, then its execution: SqlCommand answered by the relation, then
# ExecutePlan on that relation streaming its batches.
record("sql-query", {
  con <- spark_connect(remote, user_id = user_id)
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  spark_collect(q)
  spark_disconnect(con)
})

# Commands: their rows arrive in the SqlCommandResult.
record("sql-command", {
  con <- spark_connect(remote, user_id = user_id)
  spark_sql(con, "SHOW DATABASES")
  spark_sql(con, "CREATE OR REPLACE TEMPORARY VIEW zuspark_v AS SELECT 1 AS x")
  spark_disconnect(con)
})

# A query with no rows: one batch carrying only the schema.
record("sql-empty", {
  con <- spark_connect(remote, user_id = user_id)
  spark_collect(spark_sql(con, "SELECT id FROM range(3) WHERE id > 99"))
  spark_disconnect(con)
})

# A result cut by the server several times, at 100-row batches.
record("reattach", {
  con <- spark_connect(cutting, user_id = user_id)
  spark_config(con, spark.sql.execution.arrow.maxRecordsPerBatch = 100)
  q <- spark_sql(con, "SELECT id FROM range(1000)")
  spark_collect(q)
  stopifnot(q$reattaches >= 2L)
  spark_disconnect(con)
})
