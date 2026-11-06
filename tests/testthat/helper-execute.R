# Helpers and data for test-execute.R, kept out of the test file so that
# devtools::test(shuffle = TRUE) cannot run a test before them.

reattach_sql <- "SELECT id FROM range(1000)"
