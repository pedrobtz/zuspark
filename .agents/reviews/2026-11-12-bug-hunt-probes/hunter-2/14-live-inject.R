source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/hunt-2/live.R")
transport_cnd <- function(message = "connection reset") structure(class = c("zugrpc_error_transport", "zugrpc_error_unavailable", "zugrpc_error", "error", "condition"), list(message = message, call = NULL))
px <- new_proxy(); con <- px$connect(MAIN)
spark_sql(con, "CREATE TABLE IF NOT EXISTS hunt2_inj (a BIGINT) USING parquet"); spark_sql(con, "TRUNCATE TABLE hunt2_inj")
count <- function() { h <- px$hooks; px$hooks <- list(); on.exit(px$hooks <- h); spark_collect(spark_sql(con, "SELECT count(*) n FROM hunt2_inj"))$n }
# cut every ExecutePlan/Reattach stream of the watched statement at read index k (0 = before any response)
cut_at <- function(ks, cancel = TRUE) { fired <- 0L; function(method, n, s, id) {
  if (fired < length(ks) && n == ks[[fired + 1L]]) { fired <<- fired + 1L; if (cancel) s$cancel(); stop(transport_cnd()) } } }
expect <- 0
for (cancel in c(TRUE, FALSE)) for (ks in list(0, c(0, 0), 1, 2, c(1, 0), c(2, 0, 0))) {
  cat("## INSERT with injected cut(s) at", paste(ks, collapse = ","), if (cancel) "(stream cancelled)" else "(stream abandoned)", "\n")
  px$log <- list(); px$hooks <- list(before_read = cut_at(ks, cancel))
  r <- try_show(spark_sql(con, "INSERT INTO hunt2_inj VALUES (1)")); px$hooks <- list(); px$show(TRUE)
  if (!inherits(r, "condition")) expect <- expect + 1
  n <- count(); cat("   rows in table:", n, " expected:", expect, if (n != expect) " <<<<< MISMATCH", "\n"); expect <- n
}
cat("## query with injected cuts at every position\n")
q <- spark_sql(con, "SELECT id FROM range(0, 40000, 1, 8)"); base <- spark_collect(q)
ok <- TRUE
for (k in 0:14) { px$log <- list(); px$hooks <- list(before_read = cut_at(c(k, 0))); d <- try_show(spark_collect(q)); px$hooks <- list()
  same <- identical(d, base); ok <- ok && same; if (!same) { cat("   k =", k, "differs\n"); px$show(TRUE) } }
cat("   all identical:", ok, "nrow", nrow(base), "\n"); px$show(TRUE)
spark_sql(con, "DROP TABLE hunt2_inj")
spark_disconnect(con)
