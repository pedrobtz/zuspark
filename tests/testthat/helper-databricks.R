# Helpers and data for test-databricks.R, kept out of the test file so that
# devtools::test(shuffle = TRUE) cannot run a test before them.

local_connect_args <- function(env = parent.frame()) {
  seen <- new.env(parent = emptyenv())
  local_mocked_bindings(
    spark_connect = function(url, ...) {
      seen$url <- url
      seen$args <- list(...)
      invisible(NULL)
    },
    .env = env
  )
  seen
}
