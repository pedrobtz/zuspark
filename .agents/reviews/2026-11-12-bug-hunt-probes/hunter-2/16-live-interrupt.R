source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/hunt-2/live.R")
RINT <- structure(class = c("interrupt", "condition"), list(message = "", call = NULL))
px <- new_proxy(); con <- px$connect(MAIN)
real <- con$transport
con$transport <- function(con, method, bytes, deadline, streaming = FALSE) withCallingHandlers(real(con, method, bytes, deadline, streaming), error = function(e) cat("   [", method, "failed:", paste(head(class(e), 3), collapse = "/"), substr(conditionMessage(e), 1, 120), "| ErrorInfo domain:", e$details$details$ErrorInfo$domain, "| as zuspark:", class(zuspark:::as_zuspark_error(e))[1], "]\n"))
for (at in c(0L, 1L)) {
  cat("## interrupt before read", at, "\n")
  q <- spark_sql(con, "SELECT CASE WHEN java_method('java.lang.Thread', 'sleep', 300L) IS NULL THEN id ELSE id END x FROM range(0, 60, 1, 1)"); px$log <- list()
  px$hooks <- list(before_read = function(method, n, s, id) if (n == at) stop(RINT))
  e <- try_show(spark_collect(q)); px$hooks <- list(); px$show(TRUE)
  Sys.sleep(1); system(sprintf("podman logs hunt2-spark 2>&1 | grep '%s' | grep 'ExecuteHolder\\|Release all\\|Abandon' | cut -c1-200", q$operation_id))
}
spark_disconnect(con)
