.libPaths(c("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/lib", .libPaths()))
suppressMessages(library(zuspark))
URL <- "sc://127.0.0.1:16002"
t <- function(expr, show = TRUE) { r <- tryCatch(withCallingHandlers(expr, warning=function(w){cat("  WARN:", conditionMessage(w), "\n"); invokeRestart("muffleWarning")}), error = function(e) structure(list(paste(class(e), collapse="/"), conditionMessage(e)), class="err")); if (inherits(r,"err")) cat("  ERR[", r[[1]], "]", r[[2]], "\n") else if (show) {if (is.data.frame(r)) print(r) else str(r)}; invisible(r) }
# spy transport: records decoded requests
spy <- new.env(); spy$calls <- list()
spy_transport <- function(con, method, bytes, deadline, streaming = FALSE) {
  spy$calls[[length(spy$calls) + 1L]] <- list(method = method, bytes = bytes, deadline = deadline)
  zuspark:::grpc_transport(con, method, bytes, deadline, streaming)
}
dec <- function(call) { ty <- sprintf("spark.connect.%sRequest", call$method); zuproto::proto_decode(call$bytes, ty, zuspark:::spark_pool(), enums = "character") }
last_req <- function(method = NULL) { cs <- spy$calls; if (!is.null(method)) cs <- Filter(function(c) c$method == method, cs); dec(cs[[length(cs)]]) }
