HUNT <- "/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt"
.libPaths(c(file.path(HUNT, "lib"), .libPaths()))
suppressPackageStartupMessages(library(zuspark))
pool <- zuspark:::spark_pool()
dec <- function(b, type) zuproto::proto_decode(b, paste0("spark.connect.", type), pool, enums = "character")
`%||%` <- function(x, y) if (is.null(x)) y else x
# Recording proxy over the real transport. hooks: before(method, req) may signal; on_message(method, msg, n) may signal or return replacement
new_proxy <- function(hooks = list()) {
  px <- new.env(); px$log <- list(); px$hooks <- hooks
  px$transport <- function(con, method, bytes, deadline, streaming = FALSE) {
    req <- dec(bytes, paste0(method, "Request"))
    id <- length(px$log) + 1L
    px$log[[id]] <- list(method = method, req = req, responses = list())
    if (!is.null(px$hooks[["before"]])) px$hooks[["before"]](method, req, id)
    if (!streaming) {
      r <- zuspark:::grpc_transport(con, method, bytes, deadline, FALSE)
      px$log[[id]]$responses <- list(dec(r, paste0(method, "Response")))
      if (!is.null(px$hooks[["after"]])) px$hooks[["after"]](method, req, id)
      return(r)
    }
    s <- zuspark:::grpc_transport(con, method, bytes, deadline, TRUE)
    n <- 0L
    list(next_message = function() {
      if (!is.null(px$hooks$before_read)) px$hooks$before_read(method, n, s, id)
      m <- s$next_message()
      if (!is.null(m)) { n <<- n + 1L; px$log[[id]]$responses[[n]] <- dec(m, "ExecutePlanResponse") } else px$log[[id]]$ended <- TRUE
      m
    }, cancel = s$cancel)
  }
  px$connect <- function(url, ...) { options(zuspark.transport = px$transport); con <- spark_connect(url, ...); px$log <- list(); con }
  px$show <- function(detail = FALSE) for (l in px$log) {
    r <- l$req
    kinds <- vapply(l$responses, function(x) paste(setdiff(names(x)[vapply(x, function(v) length(v) > 0, NA)], c("session_id", "server_side_session_id", "operation_id", "response_id")), collapse = "+"), "")
    cat("   >", l$method, c(if (nzchar(r$operation_id %||% "")) paste0("op=", substr(r$operation_id, 1, 8)),
      if (!is.null(r$last_response_id)) paste0("last=", substr(r$last_response_id, 1, 8)),
      if (!is.null(r$release_until)) paste0("until=", substr(r$release_until$response_id, 1, 8)),
      if (!is.null(r$release_all)) "ALL", if (!is.null(r$interrupt_type)) r$interrupt_type),
      "| got", length(l$responses), if (detail) paste(kinds, collapse = " "), "\n")
  }
  px
}
try_show <- function(expr) {
  r <- tryCatch(expr, error = function(e) e, interrupt = function(e) e)
  if (inherits(r, "condition")) cat("   ERROR <", paste(head(class(r), 3), collapse = "/"), ">", substr(conditionMessage(r), 1, 300), "\n")
  r
}
MAIN <- "sc://127.0.0.1:16012"; CUT <- "sc://127.0.0.1:16013"
