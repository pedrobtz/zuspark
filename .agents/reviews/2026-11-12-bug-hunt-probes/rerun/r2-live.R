.libPaths(c("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/lib2", .libPaths()))
suppressPackageStartupMessages({ library(zuspark); library(DBI) })
url <- "sc://127.0.0.1:16032"
check <- function(id, reproduced, note = "") cat(sprintf("%-4s %s  %s\n", id, if (isTRUE(reproduced)) "REPRODUCED" else "not reproduced", note))

d <- dbConnect(spark_driver(), url)
# A: ${} substitution
s <- c("price ${amount}", "${spark.app.name}", "plain")
dbWriteTable(d, "rr_a", data.frame(s = s), temporary = TRUE, overwrite = TRUE)
got <- dbReadTable(d, "rr_a")$s
check("A", !identical(got, s), paste(got, collapse = " | "))
got2 <- dbGetQuery(d, paste("SELECT", dbQuoteString(d, "${spark.app.name}"), "AS s"))$s
check("A2", !identical(got2, "${spark.app.name}"), got2)
got3 <- tryCatch(names(dbGetQuery(d, paste("SELECT 1 AS", dbQuoteIdentifier(d, "${spark.app.name}")))), error = function(e) conditionMessage(e))
check("A3", !identical(got3, "${spark.app.name}"), paste("identifier ->", got3))

# B: dbplyr backslashes
suppressPackageStartupMessages({ library(dplyr); library(dbplyr) })
b <- c("back\\slash", "C:\\new\\table")
dbWriteTable(d, "rr_b", data.frame(s = b), temporary = TRUE, overwrite = TRUE)
n <- tbl(d, "rr_b") |> filter(s == "back\\slash") |> collect() |> nrow()
check("B", n == 0L, paste("filter rows:", n))
copy_to(d, data.frame(s = b), "rr_b2", overwrite = TRUE)
got <- dbReadTable(d, "rr_b2")$s
check("B2", !identical(got, b), paste(encodeString(got), collapse = " | "))

# E live
con <- spark_connect(url)
e <- tryCatch(spark_sql(con, "SELECT sum(sum(id)) FROM range(3)"), error = identity)
check("E", length(e$message_parameters) == 1L && is.na(e$message_parameters), paste(e$error_class, deparse(e$message_parameters)))

# F live: integer64 without bit64 loaded
x <- structure(c(4.94065645841247e-324, 2.47032822920623e-323), class = "integer64")
v <- dbGetQuery(d, paste("SELECT", dbQuoteLiteral(d, x[1]), "AS v"))$v
check("F", !isNamespaceLoaded("bit64") && identical(as.numeric(v), 0), paste("stored:", v))

# W: the last DBI result's rows travel inside a saved query
q <- spark_sql(con, "SELECT 1 AS x")  # unrelated query on the DBI connection's con
dq <- spark_sql(d@con, "SELECT 1 AS x")
before <- length(serialize(dq, NULL))
invisible(dbGetQuery(d, "SELECT id, 'secret-row-data' s FROM range(20000)"))
after <- length(serialize(dq, NULL))
check("W", after > 10 * before && length(grepRaw("secret-row-data", serialize(dq, NULL), fixed = TRUE)) > 0, sprintf("%d -> %d bytes", before, after))

# M2: non-ASCII user_id in the URL, live
m2 <- tryCatch({ k <- spark_connect(paste0(url, "/;user_id=zo%C3%AB")); spark_disconnect(k); "ok" }, error = function(e) paste(class(e)[1:2], collapse = "/"))
check("M2", !identical(m2, "ok"), m2)

# U: fork child gc releases parent's session
f <- spark_connect(url)
j <- parallel::mcparallel({ rm(f, envir = globalenv()); gc(); 0 })
invisible(parallel::mccollect(j))
u <- tryCatch({ spark_sql(f, "SELECT 1"); "alive" }, error = function(e) e$error_class %||% conditionMessage(e))
check("U", !identical(u, "alive"), paste("parent after child gc:", u, "; released flag:", f$released))

# V live: POSIXct deadline expires for the connection's life; disconnect leaks the session
k <- spark_connect(url, deadline = Sys.time() + 3)
Sys.sleep(4)
v1 <- tryCatch({ spark_config(k, "spark.app.name"); "ok" }, error = function(e) paste(class(e)[1], conditionMessage(e)))
check("V", !identical(v1, "ok"), v1)
sid <- spark_session_id(k)[["client"]]
v2 <- tryCatch({ spark_disconnect(k); "ok" }, error = function(e) class(e)[1])
cat("     V disconnect:", v2, "session", sid, "\n")

# X: gc of the original connection kills an unserialized copy
q2 <- (function() { c0 <- spark_connect(url); unserialize(serialize(spark_sql(c0, "SELECT id FROM range(10)"), NULL)) })()
invisible(gc())
x <- tryCatch({ nrow(spark_collect(q2)) }, error = function(e) paste(class(e)[1], conditionMessage(e)))
check("X", !identical(x, 10L), x)

spark_disconnect(con); spark_disconnect(f)
dbDisconnect(d)
cat("V session id to look for in the log:", sid, "\n")
