.libPaths(c("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/lib2", .libPaths()))
suppressPackageStartupMessages(library(zuspark))
url <- "sc://127.0.0.1:16032"
con <- spark_connect(url)
cat("QQ '' in a literal:", spark_collect(spark_sql(con, "SELECT 'it''s' AS s"))$s, "\n")
a <- spark_connect(url); b <- spark_connect(url)
system("podman restart rerun-spark >/dev/null")
for (i in 1:60) { ok <- tryCatch({ k <- spark_connect(url); spark_disconnect(k); TRUE }, error = function(e) FALSE); if (ok) break; Sys.sleep(2) }
e <- tryCatch(spark_sql(a, "SELECT 1"), error = identity)
cat("RS verb after restart:", class(e)[1], e$error_class %||% "", "\n")
cat("RS print after restart:", capture.output(print(a))[1], "\n")
d1 <- tryCatch(spark_disconnect(a), error = function(e) class(e)[1])
d2 <- tryCatch(spark_disconnect(b), error = function(e) class(e)[1])
cat("RS disconnect after a call:", d1, "; disconnect as first call:", d2, "\n")
