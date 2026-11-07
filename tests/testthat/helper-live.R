# Live tests run against the Spark Connect server in ZUSPARK_TEST_REMOTE,
# an sc:// string. live.yaml starts one; locally, any 4.2.1 server works.
# tools/ci-live-tests.R runs them and fails if one skips.

live_remote <- function() {
  remote <- Sys.getenv("ZUSPARK_TEST_REMOTE")
  skip_if(!nzchar(remote), "ZUSPARK_TEST_REMOTE is not set")
  remote
}

local_live_connection <- function(..., env = parent.frame()) {
  con <- spark_connect(live_remote(), ...)
  withr::defer(spark_disconnect(con), envir = env)
  con
}

# A DBI connection to the live server, for test-live-dbi.R.
local_live_dbi <- function(env = parent.frame()) {
  testthat::skip_if_not_installed("DBI")
  con <- DBI::dbConnect(spark_driver(), live_remote())
  withr::defer(if (DBI::dbIsValid(con)) DBI::dbDisconnect(con), envir = env)
  con
}
