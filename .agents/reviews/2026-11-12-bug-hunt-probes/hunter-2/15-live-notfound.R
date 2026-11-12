source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/hunt-2/live.R")
transport_cnd <- function(message = "connection reset") structure(class = c("zugrpc_error_transport", "zugrpc_error_unavailable", "zugrpc_error", "error", "condition"), list(message = message, call = NULL))
RINT <- structure(class = c("interrupt", "condition"), list(message = "", call = NULL))
# a proxy whose first `drop` ExecutePlan calls never reach the server
px <- new_proxy(); con <- px$connect(MAIN)
real <- px$transport; dropn <- 0L
lossy <- function(con, method, bytes, deadline, streaming = FALSE) {
  if (method == "ExecutePlan" && dropn > 0L) { dropn <<- dropn - 1L
    px$log[[length(px$log) + 1L]] <- list(method = "ExecutePlan(DROPPED)", req = dec(bytes, "ExecutePlanRequest"), responses = list())
    return(list(next_message = function() stop(transport_cnd()), cancel = function() invisible())) }
  real(con, method, bytes, deadline, streaming)
}
con$transport <- lossy
spark_sql(con, "CREATE TABLE IF NOT EXISTS hunt2_nf (a BIGINT) USING parquet"); spark_sql(con, "TRUNCATE TABLE hunt2_nf")
for (d in 1:3) { cat("## INSERT, first", d, "ExecutePlan(s) lost before the server\n"); px$log <- list(); dropn <- d
  r <- try_show(spark_sql(con, "INSERT INTO hunt2_nf VALUES (1)")); dropn <- 0L; px$show(TRUE)
  print(spark_collect(spark_sql(con, "SELECT count(*) n FROM hunt2_nf"))) }
cat("## query, first ExecutePlan lost\n"); q <- spark_sql(con, "SELECT id FROM range(5)"); px$log <- list(); dropn <- 1L
d <- try_show(spark_collect(q)); print(d$id); px$show(TRUE); cat("   reattaches:", q$reattaches, "\n")
cat("## reattach after responses consumed, operation gone (released by a third party)\n")
q <- spark_sql(con, "SELECT id FROM range(0, 40000, 1, 8)"); px$log <- list()
px$hooks <- list(before_read = local({ done <- FALSE; function(method, n, s, id) if (!done && n == 4L) { done <<- TRUE; s$cancel()
  zuspark:::release_execute(con, px$log[[id]]$req$operation_id, all = TRUE); stop(transport_cnd()) } }))
d <- try_show(spark_collect(q)); px$hooks <- list(); px$show(TRUE); cat("   error_class:", d$error_class, "\n")
cat("## R interrupt mid-stream, live\n")
q <- spark_sql(con, "SELECT CASE WHEN java_method('java.lang.Thread', 'sleep', 300L) IS NULL THEN id ELSE id END x FROM range(0, 60, 1, 1)"); px$log <- list()
px$hooks <- list(before_read = function(method, n, s, id) if (n == 1L) stop(RINT))
t0 <- Sys.time(); e <- try_show(spark_collect(q)); px$hooks <- list(); px$show(TRUE); cat("   ids:", e$interrupted_ids, "| same op:", identical(e$interrupted_ids, q$operation_id), "| took", format(Sys.time() - t0), "\n")
cat("   server says:\n"); system(sprintf("podman logs hunt2-spark 2>&1 | grep '%s' | grep -v 'Starting for\\|accessed' | cut -c1-220 | tail -6", q$operation_id))
print(spark_collect(spark_sql(con, "SELECT 1 AS still_works")))
cat("## error details live\n")
err <- tryCatch(spark_collect(spark_sql(con, "SELECT assert_true(id < 3) FROM range(10)")), error = identity)
px$log <- list(); det <- try_show(spark_error_details(err)); cat("   n:", length(det), "\n"); for (x in det) cat("   -", x$type[1], "| class", x$error_class, "| cause", x$cause, "| stack rows", nrow(x$stack), "| ctx", nrow(x$contexts), "\n")
str(px$log[[1]]$req[c("session_id", "client_observed_server_side_session_id", "error_id")]); cat("   root idx:", px$log[[1]]$responses[[1]]$root_error_idx, "n errors", length(px$log[[1]]$responses[[1]]$errors), " cause idx:", vapply(px$log[[1]]$responses[[1]]$errors, function(e) e$cause_idx %||% NA, 0), "\n")
spark_sql(con, "DROP TABLE hunt2_nf")
spark_disconnect(con)
