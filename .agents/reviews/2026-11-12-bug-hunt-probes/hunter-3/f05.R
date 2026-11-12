source("fake.R")
tr <- function(expr) tryCatch({ expr; "ok" }, condition = function(e) paste0(class(e)[1], ": ", substr(conditionMessage(e), 1, 90)))
# straddle release_every: k batches then complete
mk_msgs <- function(k) c(rep(arrow_msgs[2], k), arrow_msgs[length(arrow_msgs)])  # k responses + result_complete
for (k in c(14, 15, 16, 17, 31, 32)) {
  reset(); con <- spark_connect(FURL); q <- spark_sql(con, "SELECT 1"); n0 <- length(log$calls) + 1L
  hooks$ExecutePlan <- function(req, n, d) { ms <- mk_msgs(k); for (i in seq_along(ms)) { ms[[i]]$response_id <- sprintf("r%03d", i); if (!is.null(ms[[i]]$arrow_batch)) ms[[i]]$arrow_batch$start_offset <- NULL }; stream_of(restamp(ms, req)) }
  r <- tr(spark_fetch(q)); cs <- log$calls[n0:length(log$calls)]
  until <- vapply(Filter(function(c) c$method == "ReleaseExecute" && is.null(c$req$release_all), cs), function(c) c$req$release_until$response_id, "")
  cat(sprintf("k=%d responses=%d -> %s | until: %s | %s\n", k, k + 1L, r, paste(until, collapse = ","), paste(calls(n0), collapse = " ")))
  spark_disconnect(con)
}
# unserialized copy of a query: interrupt on its own session refused?
reset(); con <- spark_connect(FURL); q <- spark_sql(con, "SELECT 1"); spark_collect(q); q2 <- unserialize(serialize(q, NULL))
cat("interrupt(con, copy of q): ", tr(spark_interrupt(con, q2)), "\n")
cat("interrupt(copy con, q): ", tr(spark_interrupt(q2$con, q)), "\n")
# copy outlives original's disconnect when transport is fake (transport bound, not channel)
spark_disconnect(con); n0 <- length(log$calls) + 1L
cat("copy after original disconnect: ", tr(spark_collect(q2)), " | ", paste(calls(n0), collapse = " "), "\n")
# interrupt on a never-run query, and after a failed run
reset(); con <- spark_connect(FURL); q <- spark_sql(con, "SELECT 1"); n0 <- length(log$calls) + 1L
print(spark_interrupt(con, q)); cat("calls:", paste(calls(n0), collapse = " "), "\n")
# dbConnect equivalence
library(DBI); reset()
d <- dbConnect(spark_driver(), FURL, deadline = 7, session_id = "11111111-2222-4333-8444-555555555555")
cat("dbConnect deadline:", d@con$deadline, d@con$session_id, "\n")
cat(tr(dbConnect(spark_driver(), FURL, timezone = "UTC")), "\n")
dbDisconnect(d, allow_reconnect = TRUE); cat(paste(calls(), collapse = " "), "\n")
d3 <- dbConnect(spark_driver(), con = d@con); cat("wrap released: valid", dbIsValid(d3), "\n")
