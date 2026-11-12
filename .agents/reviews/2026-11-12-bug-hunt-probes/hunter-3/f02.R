source("fake.R")
kinds <- list(R = function() rlang::interrupt(), zg = function() stop(zg("zugrpc_error_interrupt")))
run <- function(label, setup, act) {
  for (k in names(kinds)) {
    reset(); gc(); 
    boom <- kinds[[k]]
    con <- NULL
    r <- tryCatch({ con <- setup(boom); from <- length(log$calls) + 1L; act(con, boom); "no condition" },
                  condition = function(e) paste(head(class(e), 2), collapse = "/"))
    msg <- sprintf("[%s/%s] -> %s | calls: %s | chan=%d", label, k, r, paste(calls(), collapse = " "), nchan())
    if (!is.null(con)) msg <- paste0(msg, sprintf(" released=%s", con$released))
    cat(msg, "\n")
    if (!is.null(con) && !con$released) { n0 <- length(log$calls); rm(list = ls(hooks), envir = hooks); r2 <- tryCatch({spark_collect(spark_sql(con, "SELECT 1")); "usable"}, error = function(e) conditionMessage(e)); cat("    then:", r2, "\n"); try(spark_disconnect(con), silent = TRUE) }
    else { n0 <- length(log$calls); rm(con); gc(); gc(); cat("    after gc: chan=", nchan(), " extra calls: ", paste(calls(n0 + 1L), collapse = " "), "\n", sep = "") }
  }
}
plain <- function(boom) spark_connect(FURL)
run("connect:Config", function(boom) { hooks$Config <- function(req, n, d) boom(); NULL }, function(con, boom) spark_connect(FURL))
run("connect:AnalyzePlan", function(boom) { hooks$AnalyzePlan <- function(req, n, d) boom(); NULL }, function(con, boom) spark_connect(FURL))
run("unary:Config", plain, function(con, boom) { hooks$Config <- function(req, n, d) boom(); spark_config(con) })
run("stream:open", plain, function(con, boom) { hooks$ExecutePlan <- function(req, n, d) boom(); spark_sql(con, "SELECT 1") })
run("stream:read1", plain, function(con, boom) { hooks$ExecutePlan <- function(req, n, d) stream_of(list(function() boom())); spark_sql(con, "SELECT 1") })
run("stream:read-mid(collect)", plain, function(con, boom) { q <- spark_sql(con, "SELECT 1"); hooks$ExecutePlan <- function(req, n, d) stream_of(c(restamp(arrow_msgs[1:3], req), list(function() boom()))); spark_collect(q) })
run("final ReleaseExecute", plain, function(con, boom) { hooks$ReleaseExecute <- function(req, n, d) boom(); spark_sql(con, "CREATE TABLE t") })
run("Interrupt rpc after interrupt", plain, function(con, boom) { hooks$ExecutePlan <- function(req, n, d) stream_of(list(function() boom())); hooks$Interrupt <- function(req, n, d) boom(); spark_sql(con, "SELECT 1") })
run("reattach first read", plain, function(con, boom) { hooks$ExecutePlan <- function(req, n, d) stream_of(list()); hooks$ReattachExecute <- function(req, n, d) stream_of(list(function() boom())); spark_sql(con, "SELECT 1") })
run("reattach open", plain, function(con, boom) { hooks$ExecutePlan <- function(req, n, d) stream_of(list()); hooks$ReattachExecute <- function(req, n, d) boom(); spark_sql(con, "SELECT 1") })
run("disconnect:ReleaseSession", plain, function(con, boom) { hooks$ReleaseSession <- function(req, n, d) boom(); spark_disconnect(con) })
