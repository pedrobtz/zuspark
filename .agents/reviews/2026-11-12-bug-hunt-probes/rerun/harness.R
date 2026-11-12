HUNT <- "/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt"
.libPaths(c(file.path(HUNT, "lib2"), .libPaths()))
suppressPackageStartupMessages(library(zuspark))
fx <- file.path(HUNT, "src/tests/testthat/fixtures")
rd <- function(p) readBin(p, "raw", file.size(p))
pool <- zuspark:::spark_pool()
enc <- function(x, type) zuproto::proto_encode(x, paste0("spark.connect.", type), pool)
dec <- function(b, type) zuproto::proto_decode(b, paste0("spark.connect.", type), pool, enums = "character")
SERVER_SID <- "82d48c7f-8640-4e71-87e7-762cc99e46e4"

# real arrow batches from the sql-query fixture: rows 1,1,1,2 (ids 0..4)
fixture_batches <- local({
  fs <- sort(list.files(file.path(fx, "sql-query"), "^05-ExecutePlan\\.response", full.names = TRUE))
  out <- list()
  for (f in fs) { x <- dec(rd(f), "ExecutePlanResponse"); if (!is.null(x$arrow_batch)) out[[length(out) + 1]] <- x$arrow_batch }
  out
})
sql_relation <- dec(rd(file.path(fx, "sql-query", "03-ExecutePlan.response.001.binpb")), "ExecutePlanResponse")$sql_command_result$relation

transport_cnd <- function(message = "connection reset") structure(
  class = c("zugrpc_error_transport", "zugrpc_error_unavailable", "zugrpc_error", "error", "condition"),
  list(message = message, call = NULL))
status_cnd <- function(status, message = status) structure(
  class = c(paste0("zugrpc_error_", tolower(status)), "zugrpc_error_status", "zugrpc_error", "error", "condition"),
  list(message = paste0(status, ": ", message), call = NULL, grpc_status = status))
spark_cnd <- function(error_class = "TABLE_OR_VIEW_NOT_FOUND", message = paste0("[", error_class, "] boom"), domain = "org.apache.spark", status = "INTERNAL") {
  cnd <- status_cnd(status, message); cnd$grpc_message <- message
  cnd$details <- list(code = 13L, message = message, details = list(ErrorInfo = list(
    reason = "org.apache.spark.SparkException", domain = domain,
    metadata = c(classes = "[\"org.apache.spark.SparkException\"]", errorClass = error_class,
      errorId = "03dec9d6-18b5-422a-a050-f3c37925a892", messageParameters = "{}", sqlState = "42P01"))))
  cnd
}

# A scripted server. `streams`: list, one per streaming call in order; each a
# list of items: a list (response fields; header filled in), raw bytes, a
# condition (signalled), or a function(req) returning such a list lazily.
# `unary`: named list of method -> function(req, srv) returning fields/bytes or signalling.
new_server <- function(streams = list(), unary = list()) {
  srv <- new.env()
  srv$log <- list(); srv$streams <- streams; srv$k <- 0L; srv$cancels <- 0L
  srv$transport <- function(con, method, bytes, deadline, streaming = FALSE) {
    req <- dec(bytes, paste0(method, "Request"))
    srv$log[[length(srv$log) + 1L]] <- list(method = method, req = req, deadline = deadline)
    hdr <- list(session_id = req$session_id, server_side_session_id = SERVER_SID)
    if (streaming) {
      srv$k <- srv$k + 1L
      if (srv$k > length(srv$streams)) stop(sprintf("FAKE: unscripted stream %d (%s)", srv$k, method))
      items <- srv$streams[[srv$k]]
      if (is.function(items)) items <- items(req)
      if (inherits(items, "condition")) stop(items)
      i <- 0L
      return(list(next_message = function() {
        i <<- i + 1L
        if (i > length(items)) return(NULL)
        m <- items[[i]]
        if (inherits(m, "condition")) stop(m)
        if (is.raw(m)) return(m)
        h <- c(hdr, list(operation_id = req$operation_id))
        for (n in names(m)) h[[n]] <- m[[n]]
        if (is.null(h$response_id)) h$response_id <- sprintf("r-%d-%d", srv$k, i)
        if (identical(h$response_id, NA)) h$response_id <- NULL
        enc(h, "ExecutePlanResponse")
      }, cancel = function() { srv$cancels <- srv$cancels + 1L; invisible() }))
    }
    if (!is.null(unary[[method]])) {
      r <- unary[[method]](req, srv)
      if (is.raw(r)) return(r)
      for (n in names(r)) hdr[[n]] <- r[[n]]
      return(enc(hdr, paste0(method, "Response")))
    }
    switch(method,
      Config = enc(hdr, "ConfigResponse"),
      AnalyzePlan = enc(c(hdr, list(spark_version = list(version = "4.2.1"))), "AnalyzePlanResponse"),
      ReleaseExecute = enc(c(hdr, list(operation_id = req$operation_id)), "ReleaseExecuteResponse"),
      ReleaseSession = enc(hdr, "ReleaseSessionResponse"),
      Interrupt = enc(c(hdr, list(interrupted_ids = req$operation_id)), "InterruptResponse"),
      stop("FAKE: unscripted unary ", method))
  }
  srv$connect <- function(...) {
    options(zuspark.transport = srv$transport)
    con <- spark_connect("sc://fake.invalid", user_id = "hunter", ...)
    srv$log <- list()
    con
  }
  srv$methods <- function() vapply(srv$log, function(l) l$method, "")
  srv$show <- function() {
    for (l in srv$log) {
      r <- l$req
      extra <- c(
        if (nzchar(r$operation_id %||% "")) paste0("op=", substr(r$operation_id, 1, 8)),
        if (!is.null(r$last_response_id)) paste0("last=", r$last_response_id),
        if (!is.null(r$release_until)) paste0("until=", r$release_until$response_id),
        if (!is.null(r$release_all)) "ALL",
        if (!is.null(r$interrupt_type)) r$interrupt_type,
        if (!nzchar(r$client_observed_server_side_session_id %||% "")) "NO-OBSERVED-SID")
      cat("   >", l$method, extra, "\n")
    }
  }
  srv
}
`%||%` <- function(x, y) if (is.null(x)) y else x
# convenience response items
B <- function(i, offset = NULL, ...) { b <- fixture_batches[[i]]; if (!is.null(offset)) b$start_offset <- offset; list(arrow_batch = b, ...) }
RC <- function(...) list(result_complete = list(), ...)
PROG <- function(...) list(execution_progress = list(num_inflight_tasks = 1), ...)
new_q <- function(con) zuspark:::new_query(con, "SELECT fake", sql_relation)
try_show <- function(expr) {
  r <- tryCatch(expr, error = function(e) e, interrupt = function(e) e)
  if (inherits(r, "condition")) cat("   ERROR <", paste(head(class(r), 3), collapse = "/"), ">", conditionMessage(r), "\n")
  r
}
