source("pre.R")
P <- zuspark:::spark_pool()
enc <- function(x, type) zuproto::proto_encode(x, paste0("spark.connect.", type), P)
dec <- function(b, type) zuproto::proto_decode(b, paste0("spark.connect.", type), P, enums = "character")
FX <- "../src/tests/testthat/fixtures"
rb <- function(f) readBin(f, "raw", file.size(f))
# arrow payloads from sql-query fixture call 5
arrow_msgs <- lapply(sort(list.files(file.path(FX, "sql-query"), "^05-ExecutePlan\\.response", full.names = TRUE)), function(f) dec(rb(f), "ExecutePlanResponse"))
sqlcmd_msgs <- lapply(sort(list.files(file.path(FX, "sql-query"), "^03-ExecutePlan\\.response", full.names = TRUE)), function(f) dec(rb(f), "ExecutePlanResponse"))
SSID <- "99999999-9999-4999-8999-999999999999"
log <- new.env(); log$calls <- list()
reqtype <- c(Config="ConfigRequest", AnalyzePlan="AnalyzePlanRequest", ReleaseSession="ReleaseSessionRequest", ExecutePlan="ExecutePlanRequest", ReattachExecute="ReattachExecuteRequest", ReleaseExecute="ReleaseExecuteRequest", FetchErrorDetails="FetchErrorDetailsRequest", Interrupt="InterruptRequest")
hooks <- new.env()   # hooks[[method]] <- function(req, n, default) ...
stream_of <- function(msgs) { i <- 0L; st <- new.env(); st$cancelled <- 0L
  list(next_message = function() { i <<- i + 1L; if (i > length(msgs)) return(NULL); m <- msgs[[i]]; if (is.function(m)) m <- m(); if (inherits(m, "condition")) stop(m); m },
       cancel = function() { log$calls[[length(log$calls) + 1L]] <- list(method = "<cancel>"); invisible() }) }
restamp <- function(msgs, req) lapply(seq_along(msgs), function(i) { m <- msgs[[i]]; m$session_id <- req$session_id; m$server_side_session_id <- SSID; m$operation_id <- req$operation_id; enc(m, "ExecutePlanResponse") })
default <- function(method, req) {
  hdr <- list(session_id = req$session_id, server_side_session_id = SSID)
  switch(method,
    Config = enc(hdr, "ConfigResponse"),
    AnalyzePlan = enc(c(hdr, list(spark_version = list(version = "4.2.1"))), "AnalyzePlanResponse"),
    ReleaseSession = enc(hdr, "ReleaseSessionResponse"),
    ReleaseExecute = enc(c(hdr, list(operation_id = req$operation_id)), "ReleaseExecuteResponse"),
    Interrupt = enc(c(hdr, list(interrupted_ids = list(req$operation_id %||% "x"))), "InterruptResponse"),
    ExecutePlan = stream_of(restamp(if (!is.null(req$plan$command)) sqlcmd_msgs else arrow_msgs, req)),
    ReattachExecute = stream_of(list()),
    stop("no default for ", method))
}
`%||%` <- function(a, b) if (is.null(a)) b else a
fake <- function(con, method, bytes, deadline, streaming = FALSE) {
  req <- dec(bytes, reqtype[[method]])
  n <- length(log$calls) + 1L
  log$calls[[n]] <- list(method = method, req = req, deadline = deadline, streaming = streaming)
  h <- hooks[[method]]
  if (!is.null(h)) return(h(req, sum(vapply(log$calls, function(c) c$method == method, NA)), function() default(method, req)))
  default(method, req)
}
options(zuspark.transport = fake)
calls <- function(from = 1L) { cs <- log$calls[seq_len(length(log$calls)) >= from]; vapply(cs, function(c) { x <- c$method
  if (x == "ReleaseSession") x <- sprintf("ReleaseSession(allow=%s,dl=%s)", c$req$allow_reconnect %||% "unset", format(c$deadline))
  if (x == "ReleaseExecute") x <- sprintf("ReleaseExecute(%s)", if (!is.null(c$req$release_all)) "all" else "until")
  if (x == "Interrupt") x <- sprintf("Interrupt(%s)", c$req$interrupt_type)
  if (x == "ReattachExecute") x <- sprintf("Reattach(last=%s)", substr(c$req$last_response_id %||% "none", 1, 8))
  x }, "") }
reset <- function() { log$calls <- list(); rm(list = ls(hooks), envir = hooks) }
zg <- function(class, msg = class) structure(class = c(class, "zugrpc_error", "error", "condition"), list(message = msg, call = NULL))
FURL <- "sc://127.0.0.1:1"
