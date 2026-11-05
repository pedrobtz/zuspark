# Runs the live tests (tests/testthat/test-live-*.R) against the installed
# package and the server in ZUSPARK_TEST_REMOTE. A test that skips here is
# a failure: in live.yaml every live test must run.
if (!nzchar(Sys.getenv("ZUSPARK_TEST_REMOTE"))) {
  stop("ZUSPARK_TEST_REMOTE is not set")
}
results <- as.data.frame(testthat::test_dir(
  "tests/testthat",
  package = "zuspark",
  load_package = "installed",
  filter = "^live-",
  reporter = "summary",
  stop_on_failure = TRUE
))
if (nrow(results) == 0L || any(results$skipped)) {
  stop(
    "Live tests were skipped: ",
    paste(results$test[results$skipped], collapse = "; ")
  )
}
cat(sprintf("%d live tests ran\n", nrow(results)))
