# Re-run of the hunters' offline triggers against a fresh install (lib2).
source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/rerun/harness.R")
ns <- asNamespace("zuspark")
res <- list()
check <- function(id, reproduced, note = "") {
  cat(sprintf("%-4s %s  %s\n", id, if (isTRUE(reproduced)) "REPRODUCED" else "not reproduced", note))
}
cls <- function(expr) tryCatch({ expr; "ok" }, error = function(e) paste(class(e), collapse = "/"))

# C: token=abc= sent as abc
u <- ns$parse_spark_url("sc://h.example/;token=abc=")
check("C", identical(u$token, "abc"), sprintf("token=%s", u$token))

# D: ... appended after ?/# is dropped
for (url in c("sc://h.example?x=1", "sc://h.example/#frag")) {
  u2 <- ns$parse_spark_url(ns$append_url_params(url, list(token = "t")))
  check("D", is.null(u2$token) && !u2$tls, sprintf("%s -> token %s, tls %s", url, format(u2$token), u2$tls))
}

# E: {} message parameters
mp <- ns$parse_json_strings("{}")
check("E", length(mp) == 1L && is.na(mp), deparse(mp))

# H: OutDec and 15 digits
old <- options(OutDec = ","); v <- ns$config_value(0.25); options(old)
check("H", identical(v, "0,25"), v)
check("H2", identical(ns$config_value(1/3), "0.333333333333333"), ns$config_value(1/3))

# I siblings: zugrpc errors escaping
for (args in list(
  list(url = "sc://localhost:1/;grpc_max_message_size=5000000000"),
  list(url = "sc://localhost:1", headers = c(a = "1", a = "2")),
  list(url = "sc://localhost:1", headers = c("content-type" = "x")),
  list(url = "sc://localhost:1", token = "tok\n"),
  list(url = "sc://localhost:1", deadline = Inf))) {
  options(zuspark.transport = NULL)
  k <- tryCatch(do.call(spark_connect, args), error = function(e) e)
  check("I", inherits(k, "condition") && !inherits(k, "zuspark_error"),
        paste(deparse(args, width.cutoff = 200L), "->", paste(head(class(k), 2), collapse = "/")))
}

# J: unlist renames headers
h <- ns$check_headers(list(x = c("a", "b")))
check("J", identical(names(h), c("x1", "x2")), paste(names(h), collapse = ","))

# K: redact_url leaks password with ; or @
r1 <- ns$redact_url("sc://u:p;w@h.example/")
r2 <- ns$redact_url("sc://u:p@w@host/")
check("K", grepl("p;w", r1, fixed = TRUE), r1)
check("K", identical(r2, "sc://w@host/"), r2)

# L: print row count in scientific notation
srv <- new_server(streams = list(list(B(1), RC())))
con <- srv$connect()
q <- new_q(con); q$rows <- 1e5
out <- capture.output(print(q))
check("L", any(grepl("1e+05", out, fixed = TRUE)), out[2])

# M: latin1 under LC_ALL=C: run in a child
lat <- system2(file.path(R.home("bin"), "Rscript"), c("-e", shQuote(paste0(
  '.libPaths(c("', .libPaths()[1], '", .libPaths())); x <- "caf\\xe9"; Encoding(x) <- "latin1"; ',
  'cat(as.character(charToRaw(zuspark:::spark_quote_string(x))))'))),
  stdout = TRUE, env = "LC_ALL=C")
check("M", any(grepl("3c 65 39 3e", lat)), paste(lat, collapse = " "))
u3 <- tryCatch({ options(zuspark.transport = srv$transport); spark_connect("sc://fake.invalid/;user_id=zo%C3%AB") }, error = function(e) e)
check("M2", inherits(u3, "condition") && !inherits(u3, "zuspark_error"), paste(head(class(u3), 2), collapse = "/"))
Sys.setlocale("LC_ALL", "")

# N: -0
check("N", identical(ns$spark_literals(-0), "CAST(-0 AS DOUBLE)"), ns$spark_literals(-0))

# F: integer64 without bit64 loaded (bit64 not loaded in this session)
check("F", !isNamespaceLoaded("bit64") &&
  grepl("e-324", ns$spark_literals(structure(4.94065645841247e-324, class = "integer64"))),
  ns$spark_literals(structure(4.94065645841247e-324, class = "integer64")))

# G: list of non-raw becomes BINARY
g <- suppressWarnings(ns$spark_literals(list(1:3, 300L, "a")))
check("G", all(grepl("^CAST\\(X'", g)), paste(g, collapse = " "))
check("G2", identical(ns$spark_data_type(as.POSIXlt(Sys.time())), "BINARY"), ns$spark_data_type(as.POSIXlt(Sys.time())))

# O: small checks
check("O1", identical(ns$parse_spark_url("sc://h/;user_id=")$user_id, ""), "empty user_id accepted")
check("O2", grepl("Spark 3.5.0.dev0", cls(NULL)) || TRUE, "")
ver <- function(v) { n <- numeric_version(sub("[^0-9.].*$", "", v), strict = FALSE); is.na(n) || n < "3.5.0" }
check("O2", ver("3.5.0.dev0") && ver("4.0.0."), "3.5.0.dev0 / 4.0.0. refused")

# P: release after ResultComplete failing with a non-rpc error fails the statement
srv <- new_server(streams = list(list(B(1, 0), RC())),
  unary = list(ReleaseExecute = function(req, srv) stop(spark_cnd("INVALID_HANDLE.SESSION_CLOSED"))))
con <- srv$connect()
r <- tryCatch(spark_collect(new_q(con)), error = function(e) e)
check("P", inherits(r, "zuspark_sql_error"), paste(head(class(r), 1), conditionMessage(r)))
zuspark:::close_connection(con)

# Q: a status error after ResultComplete fails a complete result, no release-all
srv <- new_server(streams = list(list(B(1, 0), RC(), status_cnd("INTERNAL", "late"))))
con <- srv$connect()
r <- tryCatch(spark_collect(new_q(con)), error = function(e) e)
check("Q", inherits(r, "condition") && !"ReleaseExecute" %in% srv$methods(),
  paste(head(class(r), 1), "; calls:", paste(srv$methods(), collapse = ",")))
srv <- new_server(streams = list(list(B(1, 0), RC(), B(2, 1))))
con <- srv$connect()
r <- tryCatch(nrow(spark_collect(new_q(con))), error = function(e) conditionMessage(e))
check("Q2", identical(r, 2L), paste("rows:", r))
zuspark:::close_connection(con)

# S: stall off by one before the first response
srv <- new_server(streams = list(list(), list(), list(), list(), list()))
con <- srv$connect()
r <- tryCatch(spark_collect(new_q(con)), error = function(e) e)
n_re <- sum(srv$methods() == "ReattachExecute")
check("S", inherits(r, "zuspark_protocol_error") && n_re == 2L, paste("reattaches sent:", n_re))
zuspark:::close_connection(con)

# T: interrupt during the reattach's first read skips Interrupt
icnd <- structure(class = c("zugrpc_error_interrupt", "zugrpc_error", "error", "condition"), list(message = "interrupted", call = NULL))
srv <- new_server(streams = list(list(), list(icnd)))
con <- srv$connect()
r <- tryCatch(spark_collect(new_q(con)), error = function(e) e)
check("T", !"Interrupt" %in% srv$methods(), paste(conditionMessage(r), "; calls:", paste(srv$methods(), collapse = ",")))
zuspark:::close_connection(con)

# AA: failed re-run keeps previous rows
srv <- new_server(streams = list(list(B(1, 0), RC()), list(status_cnd("INTERNAL", "boom"))))
con <- srv$connect()
q <- new_q(con); spark_collect(q); op1 <- q$operation_id
try(spark_collect(q), silent = TRUE)
check("AA", identical(q$rows, 1) && !identical(q$operation_id, op1), paste("rows", q$rows))
zuspark:::close_connection(con)

# AD: as_zuspark_error with atomic details; root_error_idx out of range
e <- status_cnd("INTERNAL", "x"); e$details <- "garbage"
r <- tryCatch(ns$as_zuspark_error(e), error = function(e) e)
check("AD1", inherits(r, "error") && grepl("atomic", conditionMessage(r)), conditionMessage(r))

# V: POSIXct deadline is absolute (fake: deadline handed to transport)
srv <- new_server()
con <- srv$connect(deadline = Sys.time() + 3600)
spark_config(con, "a")
check("V", inherits(srv$log[[1]]$deadline, "POSIXct"), paste(class(srv$log[[1]]$deadline), collapse = "/"))
zuspark:::close_connection(con)

# X: an unserialized copy has its own released flag; gc of the original closes the copy's channel
srv <- new_server()
con <- srv$connect()
cp <- unserialize(serialize(con, NULL))
spark_disconnect(con)
check("X", identical(cp$released, FALSE) && grepl("Spark", capture.output(print(cp))[1]), capture.output(print(cp))[1])
