source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/hunt-2/harness.R")
run <- function(title, streams, unary = list(), f = spark_collect) {
  cat("##", title, "\n")
  srv <- new_server(streams, unary); con <- srv$connect(); q <- new_q(con)
  df <- try_show(f(q)); if (is.data.frame(df)) cat("   nrow", nrow(df), "ids", paste(head(df$id, 12), collapse = ","), "\n")
  cat("   rows", format(q$rows, digits = 20), "reatt", q$reattaches, "skipped", q$skipped, "| methods:", paste(srv$methods(), collapse = " "), "\n")
  invisible(list(srv = srv, q = q, df = df, con = con))
}
bb <- function(i, row_count = NULL, offset = NULL, drop_offset = FALSE, data = NULL) {
  b <- fixture_batches[[i]]
  if (!is.null(row_count)) b$row_count <- row_count
  if (!is.null(offset)) b$start_offset <- offset
  if (drop_offset) b$start_offset <- NULL
  if (!is.null(data)) b$data <- data
  list(arrow_batch = b)
}
one <- function(b) list(list(b, RC()))
run("row_count 0 but 1 row of data", one(bb(1, row_count = 0)))
run("row_count 2 but 1 row of data", one(bb(1, row_count = 2)))
run("row_count -1", one(bb(1, row_count = -1)))
run("row_counts -1 and 3 over two batches (sum right, each wrong)", list(list(bb(1, row_count = -1, drop_offset = TRUE), bb(2, row_count = 3, drop_offset = TRUE), RC())))
run("row_counts 0 and 2 (sum right, each wrong), offsets consistent with counts", list(list(bb(1, row_count = 0), bb(2, row_count = 2, offset = 0), RC())))
run("row_count 2^31", one(bb(1, row_count = 2^31)))
run("row_count 2^53", one(bb(1, row_count = 2^53)))
run("start_offset -1 on first", one(bb(1, offset = -1)))
run("start_offset 2^53+? on first", one(bb(1, offset = 2^53)))
run("start_offset wrong on 2nd", list(list(B(1), bb(2, offset = 5), RC())))
run("start_offset present on some, absent on others", list(list(B(1), bb(2, drop_offset = TRUE), B(3), B(4), RC())))
run("batch with empty data, row_count 0", list(list(B(1), bb(2, row_count = 0, data = raw(), drop_offset = TRUE), RC())))
run("only batch has empty data", one(bb(1, row_count = 0, data = raw())))
run("batch data garbage", one(bb(1, data = as.raw(1:50))))
run("spark_fetch: row_count lies (0)", one(bb(1, row_count = 0)), f = function(q) zuarrow::arrow_to_r(spark_fetch(q)))
# many batches: offsets recomputed
many <- function(n) { rows <- 0; out <- vector("list", n); for (k in seq_len(n)) { i <- ((k - 1) %% 4) + 1; out[[k]] <- B(i, offset = rows); rows <- rows + fixture_batches[[i]]$row_count }; c(out, list(RC())) }
for (n in c(15, 16, 17, 31, 32, 33, 64, 65, 200)) {
  r <- run(sprintf("%d batches", n), list(many(n)))
  exp_ids <- unlist(lapply(seq_len(n), function(k) list(0, 1, 2, c(3, 4))[[((k - 1) %% 4) + 1]]))
  cat("   data as expected:", is.data.frame(r$df) && identical(as.numeric(r$df$id), as.numeric(exp_ids)), "\n")
}
# equivalence: same result split across reattaches at every position
base <- run("base", list(many(40)))$df
ok <- TRUE
for (cut in 1:40) {
  m <- many(40)
  r <- suppressWarnings({ srv <- new_server(list(c(m[seq_len(cut)], list(transport_cnd())), m[-seq_len(cut)])); con <- srv$connect(); q <- new_q(con); spark_collect(q) })
  ok <- ok && identical(r, base) && q$reattaches == 1L && q$rows == nrow(base)
  rr <- Filter(function(l) l$method == "ReattachExecute", srv$log)[[1]]$req
  ok <- ok && identical(rr$last_response_id, sprintf("r-1-%d", cut))
}
cat("## split at every position identical & last_response_id right:", ok, "\n")
# batch buffer NULL-assignment hazard: batch$data NULL
run("spark_execute counts", list(list(list(sql_command_result = list(relation = sql_relation)), RC()), many(33)), f = function(q) { n <- spark_execute(q$con, "SELECT 1"); cat("   spark_execute ->", n, "\n"); n })
