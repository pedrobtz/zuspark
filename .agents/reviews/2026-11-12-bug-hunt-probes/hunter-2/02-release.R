source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/hunt-2/harness.R")
run <- function(title, streams, unary = list(), f = spark_collect) {
  cat("##", title, "\n")
  srv <- new_server(streams, unary); con <- srv$connect(); q <- new_q(con)
  df <- try_show(f(q)); if (is.data.frame(df)) cat("   nrow", nrow(df), "ids", paste(df$id, collapse = ","), "\n")
  srv$show(); cat("   rows", format(q$rows), "reatt", q$reattaches, "skipped", q$skipped, "cancels", srv$cancels, "\n")
  invisible(list(srv = srv, q = q, df = df, con = con))
}
P <- function(n) rep(list(PROG()), n)
all4 <- list(B(1), B(2), B(3), B(4))
for (n in c(14, 15, 16, 17, 31, 32, 33)) {
  # n responses before RC: 4 batches + (n-4) progress, then RC is response n+1
  run(sprintf("%d responses then RC", n), list(c(all4, P(n - 4), list(RC()))))
}
run("RC is the 16th response", list(c(all4, P(11), list(RC()))))
run("cut right after 16th (release due at cut)", list(c(all4, P(12), list(transport_cnd())), list(RC())))
run("cut right after 15th, 16th on reattach", list(c(all4, P(11), list(transport_cnd())), list(PROG(), RC())))
run("no response ids at all, 40 responses", list(c(lapply(all4, function(b) c(b, list(response_id = NA))), rep(list(PROG(response_id = NA)), 36), list(RC(response_id = NA)))))
run("no response ids, cut mid-way", list(list(c(B(1), list(response_id = NA)), c(B(2), list(response_id = NA)), transport_cnd()), list(B(3), B(4), RC())))
run("empty response id on 16th (id only on 3rd)", list(c(list(c(B(1), list(response_id = "")), c(B(2), list(response_id = "")), B(3, response_id = "third"), c(B(4), list(response_id = ""))), rep(list(PROG(response_id = "")), 14), list(RC()))))
cat("\n#### failing releases\n")
fail_rel <- function(cnd) list(ReleaseExecute = function(req, srv) stop(cnd))
run("inline+final release transport fail", list(c(all4, P(14), list(RC()))), fail_rel(transport_cnd()))
run("release status UNAVAILABLE", list(c(all4, P(14), list(RC()))), fail_rel(status_cnd("UNAVAILABLE")))
run("release fails with a Spark ErrorInfo (INTERNAL)", list(c(all4, list(RC()))), fail_rel(spark_cnd("INVALID_HANDLE.OPERATION_NOT_FOUND")))
run("inline release fails with a Spark ErrorInfo", list(c(all4, P(14), list(RC()))), fail_rel(spark_cnd("INVALID_HANDLE.OPERATION_NOT_FOUND")))
run("release answers with other session id", list(c(all4, list(RC()))), list(ReleaseExecute = function(req, srv) list(session_id = "other")))
run("release answers with other server-side id", list(c(all4, list(RC()))), list(ReleaseExecute = function(req, srv) list(server_side_session_id = "other")))
run("release response undecodable", list(c(all4, list(RC()))), list(ReleaseExecute = function(req, srv) as.raw(c(0xff, 0xff, 0xff))))
