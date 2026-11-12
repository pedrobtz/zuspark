source('/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/rerun/pre.R')
alive <- function(con) { r <- tc(spark_collect(spark_sql(con, "SELECT 7 AS x"))); is.data.frame(r) && identical(r$x, 7L) }
# A: child uses the connection
con <- spark_connect(URL); say("A sid %s", con$session_id)
j <- parallel::mcparallel(tryCatch(spark_collect(spark_sql(con, "SELECT 41+1 AS x"))$x, error = function(e) conditionMessage(e)))
print(parallel::mccollect(j))
say("A parent alive after child used it: %s", alive(con))
say("A parent alive again: %s", alive(con))
tc(spark_disconnect(con))
# B: child drops and gc()s
con <- spark_connect(URL); say("B sid %s", con$session_id)
j <- parallel::mcparallel({ rm(con, envir = globalenv()); invisible(gc()); invisible(gc()); "gc done" })
print(parallel::mccollect(j))
say("B parent alive after child gc: %s", alive(con))
tc(spark_disconnect(con))
# C: child just exits
con <- spark_connect(URL); say("C sid %s", con$session_id)
j <- parallel::mcparallel(1)
print(parallel::mccollect(j))
say("C parent alive after child exit: %s", alive(con))
tc(spark_disconnect(con))
# D: child disconnects explicitly
con <- spark_connect(URL); say("D sid %s", con$session_id)
j <- parallel::mcparallel(tryCatch(spark_disconnect(con), error = function(e) conditionMessage(e)))
print(parallel::mccollect(j))
say("D parent alive after child disconnect: %s (parent released flag %s)", alive(con), con$released)
tc(spark_disconnect(con))
# E: parent idle connection, child runs mclapply with fresh connection each
con <- spark_connect(URL); say("E sid %s", con$session_id)
r <- parallel::mclapply(1:2, function(i) { c2 <- spark_connect(URL); on.exit(spark_disconnect(c2)); spark_collect(spark_sql(c2, sprintf("SELECT %d AS x", i)))$x })
print(r)
say("E parent alive: %s", alive(con))
tc(spark_disconnect(con))
