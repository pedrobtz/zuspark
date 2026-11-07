# For test-branches.R, kept out of the test file so that
# devtools::test(shuffle = TRUE) cannot run a test before it.

query_sql <- function() "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)"
