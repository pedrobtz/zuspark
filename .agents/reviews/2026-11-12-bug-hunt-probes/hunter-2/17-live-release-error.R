source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/hunt-2/live.R")
px <- new_proxy(); con <- px$connect(MAIN)
q <- spark_sql(con, "SELECT CASE WHEN java_method('java.lang.Thread', 'sleep', 300L) IS NULL THEN id ELSE id END x FROM range(0, 20, 1, 1)"); px$log <- list()
got <- NULL
px$hooks <- list(before_read = function(method, n, s, id) if (n == 0L && is.null(got)) {
  got <<- tryCatch(zuspark:::release_execute(con, px$log[[id]]$req$operation_id, all = TRUE), error = function(e) e) })
d <- try_show(spark_collect(q)); px$hooks <- list(); px$show(TRUE)
cat("release_execute on a just-started operation raised:", paste(class(got), collapse = " "), "\n  error_class:", got$error_class, "\n")
cat("would release_best_effort swallow it? ", inherits(got, "zuspark_rpc_error"), "\n")
# a release for a session the server has closed
con2 <- px$connect(MAIN); sid <- spark_session_id(con2)[["client"]]
q2 <- spark_sql(con2, "SELECT 1 AS x"); 
px$hooks <- list(before = function(method, req, id) if (method == "ReleaseExecute" && identical(req$session_id, sid) && !isTRUE(px$once)) { px$once <- TRUE
  zuspark:::release_session(con2) })
px$log <- list(); d <- try_show(spark_collect(q2)); px$hooks <- list(); px$show(TRUE)
spark_disconnect(con)
