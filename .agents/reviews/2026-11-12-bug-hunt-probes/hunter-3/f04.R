source("fake.R")
show <- function(label, r, con, from = 1L) cat(sprintf("[%s] -> %s | %s | chan=%d released=%s\n", label, r, paste(calls(from), collapse = " "), nchan(), if (is.null(con)) "-" else con$released))
tr <- function(expr) tryCatch({ expr; "ok" }, condition = function(e) paste0(class(e)[1], ": ", substr(conditionMessage(e), 1, 90)))
# connect failures
reset(); hooks$Config <- function(req, n, d) stop(zg("zugrpc_error_unavailable")); show("connect Config fails", tr(spark_connect(FURL)), NULL)
reset(); hooks$AnalyzePlan <- function(req, n, d) stop(zg("zugrpc_error_unavailable")); show("connect AnalyzePlan fails", tr(spark_connect(FURL)), NULL)
reset(); hooks$AnalyzePlan <- function(req, n, d) enc(list(session_id = req$session_id, server_side_session_id = SSID, spark_version = list(version = "3.4.9")), "AnalyzePlanResponse"); show("old server", tr(spark_connect(FURL)), NULL)
reset(); hooks$AnalyzePlan <- function(req, n, d) enc(list(session_id = req$session_id, server_side_session_id = "other", spark_version = list(version = "4.2.1")), "AnalyzePlanResponse"); show("ssid changes in connect", tr(spark_connect(FURL)), NULL)
reset(); hooks$AnalyzePlan <- function(req, n, d) stop(zg("zugrpc_error_unavailable")); hooks$ReleaseSession <- function(req, n, d) rlang::interrupt(); show("connect AnalyzePlan fails, release interrupted", tr(spark_connect(FURL)), NULL)
rm(list = ls(hooks), envir = hooks); invisible(gc()); say("   after gc chan=%d calls=%s", nchan(), paste(calls(), collapse=" "))
# disconnect failures
reset(); con <- spark_connect(FURL); hooks$ReleaseSession <- function(req, n, d) stop(zg("zugrpc_error_deadline_exceeded")); show("disconnect fails", tr(spark_disconnect(con)), con)
show("  disconnect again", tr(print(spark_disconnect(con))), con)
reset(); con <- spark_connect(FURL); hooks$ReleaseSession <- function(req, n, d) stop(zg("zugrpc_error_unimplemented")); show("disconnect unimplemented", tr(print(spark_disconnect(con))), con)
reset(); con <- spark_connect(FURL); show("disconnect allow TRUE", tr(spark_disconnect(con, allow_reconnect = TRUE)), con)
# mid-stream failure: what is left
reset(); con <- spark_connect(FURL); q <- spark_sql(con, "SELECT 1"); spark_collect(q); op1 <- q$operation_id; n0 <- length(log$calls) + 1L
hooks$ExecutePlan <- function(req, n, d) stream_of(c(restamp(arrow_msgs[1:4], req), list(zg("zugrpc_error_internal", "boom"))))
show("status error mid-stream", tr(spark_collect(q)), con, n0); say("   q$rows=%s (stale) op changed=%s metrics null=%s", format(q$rows), !identical(op1, q$operation_id), is.null(q$metrics)); print(q)
# transport error mid-stream then reattach gives rest
reset(); con <- spark_connect(FURL); q <- spark_sql(con, "SELECT 1"); n0 <- length(log$calls) + 1L
hooks$ExecutePlan <- function(req, n, d) stream_of(c(restamp(arrow_msgs[1:4], req), list(zg("zugrpc_error_transport", "cut"))))
hooks$ReattachExecute <- function(req, n, d) stream_of(restamp(arrow_msgs[5:13], req))
r <- tr(df <- spark_collect(q)); show("transport cut + reattach", r, con, n0); say("   rows %d reattaches %d", nrow(df), q$reattaches)
# deadline exceeded mid-stream: is it reattached?
reset(); con <- spark_connect(FURL); q <- spark_sql(con, "SELECT 1"); n0 <- length(log$calls) + 1L
hooks$ExecutePlan <- function(req, n, d) stream_of(c(restamp(arrow_msgs[1:4], req), list(zg("zugrpc_error_deadline_exceeded", "late"))))
show("deadline mid-stream", tr(spark_collect(q)), con, n0)
# three stalls
reset(); con <- spark_connect(FURL); q <- spark_sql(con, "SELECT 1"); n0 <- length(log$calls) + 1L
hooks$ExecutePlan <- function(req, n, d) stream_of(list(zg("zugrpc_error_transport", "cut")))
hooks$ReattachExecute <- function(req, n, d) stream_of(list(zg("zugrpc_error_transport", "cut")))
show("stalls", tr(spark_collect(q)), con, n0)
# session lost noticed in final release after complete (command)
reset(); con <- spark_connect(FURL); n0 <- length(log$calls) + 1L
hooks$ReleaseExecute <- function(req, n, d) enc(list(session_id = req$session_id, server_side_session_id = "changed"), "ReleaseExecuteResponse")
show("ssid change in final release of a command", tr(spark_sql(con, "INSERT INTO t VALUES (1)")), con, n0)
# release fails with rpc error after complete: swallowed
reset(); con <- spark_connect(FURL); n0 <- length(log$calls) + 1L
hooks$ReleaseExecute <- function(req, n, d) stop(zg("zugrpc_error_unavailable"))
show("final release rpc error", tr(spark_sql(con, "SELECT 1")), con, n0)
# on_response error (bad start offset) -> cancel + release
reset(); con <- spark_connect(FURL); q <- spark_sql(con, "SELECT 1"); n0 <- length(log$calls) + 1L
hooks$ExecutePlan <- function(req, n, d) { m <- arrow_msgs; m[[2]]$arrow_batch$start_offset <- 99; stream_of(restamp(m, req)) }
show("bad offset", tr(spark_collect(q)), con, n0)
# release fails w/ session error during abandon: original error preserved?
reset(); con <- spark_connect(FURL); q <- spark_sql(con, "SELECT 1"); n0 <- length(log$calls) + 1L
hooks$ExecutePlan <- function(req, n, d) stream_of(c(restamp(arrow_msgs[1:4], req), list(zg("zugrpc_error_internal", "boom"))))
hooks$ReleaseExecute <- function(req, n, d) stop(zg("zugrpc_error_unavailable", "release failed"))
show("abandon release fails", tr(spark_collect(q)), con, n0)
# collect on fake after disconnect: nothing reaches server
reset(); con <- spark_connect(FURL); q <- spark_sql(con, "SELECT 1"); st <- spark_fetch(q); spark_disconnect(con); n0 <- length(log$calls) + 1L
r <- c(tr(spark_collect(q)), tr(spark_fetch(q)), tr(spark_schema(q)), tr(spark_interrupt(con, q)), tr(spark_config(con)), tr(spark_execute(con, "x")), tr(spark_sql(con, "x")))
show("after disconnect", paste(sub(":.*", "", r), collapse = ","), con, n0)
say("stream after disconnect rows: %d", nrow(zuarrow::arrow_to_r(st)))
