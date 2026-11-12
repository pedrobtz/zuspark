source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/rerun/harness.R")
ns <- asNamespace("zuspark")
check <- function(id, ok, note = "") cat(sprintf("%-4s %s  %s\n", id, if (isTRUE(ok)) "REPRODUCED" else "not reproduced", note))
# M1 matrix column recycles
df <- data.frame(a = 1:2); df$m <- matrix(1:4, 2)
s <- tryCatch(ns$spark_values_select(df), error = function(e) conditionMessage(e))
check("MX", lengths(regmatches(s, gregexpr("\\(CAST", s))) > 2, substr(s, 1, 160))
# double zuspark_error class
e <- tryCatch(ns$zuspark_abort("zuspark_error", "x"), error = identity)
check("DC", sum(class(e) == "zuspark_error") == 2, paste(class(e), collapse = "/"))
# duplicate response after reattach without start_offset delivered twice
b1 <- fixture_batches[[1]]; b1$start_offset <- NULL
b2 <- fixture_batches[[2]]; b2$start_offset <- NULL
srv <- new_server(streams = list(list(list(arrow_batch = b1, response_id = "x1")), list(list(arrow_batch = b1, response_id = "x1"), list(arrow_batch = b2, response_id = "x2"), RC())))
con <- srv$connect()
r <- tryCatch(nrow(spark_collect(new_q(con))), error = function(e) conditionMessage(e))
check("DUP", identical(r, 3L), paste("rows:", r, "(2 expected)"))
zuspark:::close_connection(con)
# row_count errors that cancel out pass
b1 <- fixture_batches[[1]]; b1$start_offset <- NULL; b1$row_count <- 0
b2 <- fixture_batches[[2]]; b2$start_offset <- NULL; b2$row_count <- 2
srv <- new_server(streams = list(list(list(arrow_batch = b1), list(arrow_batch = b2), RC())))
con <- srv$connect()
r <- tryCatch(nrow(spark_collect(new_q(con))), error = function(e) conditionMessage(e))
check("RC", identical(r, 2L), paste("rows:", r, "with counts 0 and 2"))
# spark_fetch does not check row_count
b1 <- fixture_batches[[1]]; b1$row_count <- 0
srv <- new_server(streams = list(list(list(arrow_batch = b1), RC())))
con <- srv$connect(); q <- new_q(con)
r <- tryCatch({ s <- spark_fetch(q); n <- nrow(zuarrow::arrow_to_r(s)); paste("rows", n, "q$rows", q$rows) }, error = function(e) conditionMessage(e))
check("FR", grepl("rows 1 q\\$rows 0", r), r)
zuspark:::close_connection(con)
# SqlCommandResult with an empty relation becomes a query
srv <- new_server(streams = list(list(list(sql_command_result = list(relation = list())), RC())))
con <- srv$connect()
r <- tryCatch(class(spark_sql(con, "SELECT 1"))[1], error = function(e) conditionMessage(e))
check("ER", identical(r, "zuspark_query"), r)
zuspark:::close_connection(con)
# client.version v-prefix
.libPaths(); suppressPackageStartupMessages(library(DBI))
check("CV", startsWith(dbGetInfo(spark_driver())$client.version, "v"), dbGetInfo(spark_driver())$client.version)
