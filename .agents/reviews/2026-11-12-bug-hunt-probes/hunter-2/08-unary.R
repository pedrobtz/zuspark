source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/hunt-2/harness.R")
all4 <- list(B(1), B(2), B(3), B(4))
INT <- structure(class = c("zugrpc_error_interrupt", "zugrpc_error", "error", "condition"), list(message = "The call was interrupted.", call = NULL, grpc_status = "CANCELLED"))
RINT <- structure(class = c("interrupt", "condition"), list(message = "", call = NULL))
run <- function(title, streams, unary = list(), f = spark_collect) {
  cat("##", title, "\n")
  srv <- new_server(streams, unary); con <- srv$connect(); q <- new_q(con)
  r <- try_show(f(q)); if (is.data.frame(r)) cat("   nrow", nrow(r), "\n")
  if (inherits(r, "zuspark_interrupt_error")) { cat("   interrupted_ids:", r$interrupted_ids, " op matches:", identical(r$operation_id, q$operation_id), "\n") }
  srv$show(); cat("   cancels", srv$cancels, "\n")
  invisible(list(srv = srv, q = q, r = r, con = con))
}
run("zugrpc interrupt mid-stream", list(list(B(1), INT)))
run("R interrupt mid-stream", list(list(B(1), RINT)))
run("interrupt; Interrupt RPC fails", list(list(B(1), INT)), list(Interrupt = function(req, srv) stop(transport_cnd())))
run("interrupt; Interrupt answers no ids", list(list(B(1), INT)), list(Interrupt = function(req, srv) list()))
run("interrupt on first read of reattach (nothing consumed)", list(list(transport_cnd()), list(INT)))
run("R interrupt on first read of reattach (nothing consumed)", list(list(transport_cnd()), list(RINT)))
run("interrupt after ResultComplete", list(c(all4, list(RC(), INT))))
r <- run("spark_interrupt(con, q) after a run", list(c(all4, list(RC()))), f = function(q) { spark_collect(q); ids <- spark_interrupt(q$con, q); cat("   ids:", ids, "\n"); spark_interrupt(q$con); spark_interrupt(q$con, "abc"); spark_interrupt(q$con, ""); 1 })
for (l in r$srv$log) if (l$method == "Interrupt") cat("   interrupt:", l$req$interrupt_type, "| op:", l$req$operation_id, "| tag:", l$req$operation_tag, "| deadline", format(l$deadline), "\n")

cat("\n#### session checks on unary answers\n")
u <- function(title, unary, f) {
  cat("##", title, "\n"); srv <- new_server(list(), unary); con <- srv$connect()
  r <- try_show(f(con)); if (!inherits(r, "condition")) str(r)
  srv$show(); invisible(list(srv = srv, con = con))
}
u("config other session id", list(Config = function(req, srv) if (length(srv$log) > 0 && !is.null(req$operation$get_option)) list(session_id = "x") else list()), function(con) spark_config(con, "a"))
u("config pairs: value absent / key absent", list(Config = function(req, srv) if (!is.null(req$operation$get_option)) list(pairs = list(list(key = "a"), list(key = "b", value = ""), list(key = "c", value = "v"))) else list()), function(con) spark_config(con, "a", "b", "c"))
u("config: fewer pairs than keys asked, other order", list(Config = function(req, srv) if (!is.null(req$operation$get_option)) list(pairs = list(list(key = "c", value = "3"), list(key = "a", value = "1"))) else list()), function(con) spark_config(con, "a", "b", "c"))
x <- u("config set+unset: requests", list(), function(con) spark_config(con, a = "1", b = NULL, k = TRUE, d = 1e5, e = NA))
for (l in x$srv$log) { cat("   op:"); str(l$req$operation, max.level = 3, give.attr = FALSE) }
u("config warnings", list(Config = function(req, srv) list(warnings = c("w1", "w2"))), function(con) withCallingHandlers(spark_config(con), warning = function(w) { cat("   WARN", conditionMessage(w), "\n"); invokeRestart("muffleWarning") }))
u("disconnect: release session fails transport", list(ReleaseSession = function(req, srv) stop(transport_cnd())), function(con) { r <- try_show(spark_disconnect(con)); cat("   released flag:", con$released, "\n"); r })
u("disconnect: server answers other server-side id", list(ReleaseSession = function(req, srv) list(server_side_session_id = "zz")), function(con) { r <- try_show(spark_disconnect(con)); cat("   released flag:", con$released, "\n"); r })
x <- u("disconnect twice, then use", list(), function(con) { a <- spark_disconnect(con); b <- spark_disconnect(con); try_show(spark_config(con)); c(a, b) })
str(x$srv$log[[1]]$req, max.level = 1)
