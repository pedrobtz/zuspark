# Records the RPC fixtures the tests replay on every leg (design §9).
#
# Run from the package root against Spark Connect 4.2.1, the image the live
# tests use:
#
#   docker run -d --name spark -p 15002:15002 -e SPARK_NO_DAEMONIZE=true \
#     apache/spark:4.2.1 /opt/spark/sbin/start-connect-server.sh \
#     --conf spark.connect.grpc.binding.address=0.0.0.0
#   Rscript tools/record-fixtures.R sc://localhost:15002
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
remote <- if (length(args)) args[[1]] else "sc://localhost:15002"
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
