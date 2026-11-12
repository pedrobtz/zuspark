source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/hunt-2/live.R")
px <- new_proxy(); con <- px$connect(CUT)
audit <- function(q) {
  # every reattach must name the last response id received before it; no response id seen twice
  ids <- character(); last <- NULL; ok <- TRUE; batches <- 0; rows <- 0; offs_ok <- TRUE
  for (l in px$log) if (identical(l$req$operation_id, q$operation_id)) {
    if (l$method == "ReattachExecute") ok <- ok && identical(l$req$last_response_id, last)
    if (l$method == "ReleaseExecute" && !is.null(l$req$release_until)) ok <- ok && l$req$release_until$response_id %in% ids
    if (l$method %in% c("ExecutePlan", "ReattachExecute")) for (r in l$responses) {
      ids <- c(ids, r$response_id); last <- r$response_id
      if (!is.null(r$arrow_batch)) { offs_ok <- offs_ok && r$arrow_batch$start_offset == rows; rows <- rows + r$arrow_batch$row_count; batches <- batches + 1 }
    }
  }
  m <- vapply(px$log, function(l) l$method, "")
  cat("   calls:", paste(names(table(m)), table(m), collapse = ", "), "| responses", length(ids), "dup ids", sum(duplicated(ids)), "| batches", batches, "rows", rows, "| reattach/release ids consistent:", ok, "| offsets ok:", offs_ok, "| q$reattaches", q$reattaches, "\n")
}
for (n in c()) {
  cat("## range(", n, ") on the cutting server\n"); px$log <- list()
  q <- spark_sql(con, sprintf("SELECT id, id %% 7 AS m FROM range(%d)", n)); px$log <- list()
  d <- try_show(spark_collect(q))
  if (is.data.frame(d)) cat("   nrow", nrow(d), "ids exact:", identical(as.numeric(d$id), as.numeric(seq_len(n) - 1)), "m exact:", identical(as.numeric(d$m), as.numeric((seq_len(n) - 1) %% 7)), "q$rows", q$rows, "\n")
  audit(q); cat("   op:", q$operation_id, "\n")
}
cat("## slow side-effect statement on the cutting server (stream ends before any response)\n")
spark_sql(con, "CREATE TABLE IF NOT EXISTS hunt2_cut (a BIGINT) USING parquet"); spark_sql(con, "TRUNCATE TABLE hunt2_cut")
px$log <- list()
r <- try_show(spark_sql(con, "INSERT INTO hunt2_cut SELECT CASE WHEN java_method('java.lang.Thread', 'sleep', 400L) IS NULL THEN id ELSE id END FROM range(0, 10, 1, 1)"))
px$show(TRUE)
print(spark_collect(spark_sql(con, "SELECT count(*) n, count(DISTINCT a) d FROM hunt2_cut")))
cat("## slow query: stream ends with nothing, several times\n")
px$log <- list(); q <- spark_sql(con, "SELECT CASE WHEN java_method('java.lang.Thread', 'sleep', 500L) IS NULL THEN id ELSE id END AS x FROM range(0, 8, 1, 1)"); px$log <- list()
d <- try_show(spark_collect(q)); print(d$x); px$show(TRUE); cat("   reattaches", q$reattaches, "\n")
spark_sql(con, "DROP TABLE hunt2_cut")
spark_disconnect(con)
