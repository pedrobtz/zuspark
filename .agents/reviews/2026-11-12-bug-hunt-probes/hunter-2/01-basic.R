source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/hunt-2/harness.R")
cat("## baseline: 4 batches one stream\n")
srv <- new_server(list(list(B(1), B(2), B(3), B(4), RC())))
con <- srv$connect(); q <- new_q(con)
df <- try_show(spark_collect(q)); print(df); srv$show(); cat("rows", q$rows, "reatt", q$reattaches, "\n")
base <- df

cat("## optional start_offset absent (Spark 3.5 style)\n")
nb <- function(i) { b <- fixture_batches[[i]]; b$start_offset <- NULL; list(arrow_batch = b) }
x <- dec(enc(list(arrow_batch = nb(2)$arrow_batch), "ExecutePlanResponse"), "ExecutePlanResponse"); str(x$arrow_batch$start_offset)
srv <- new_server(list(list(nb(1), nb(2), nb(3), nb(4), RC())))
con <- srv$connect(); q <- new_q(con)
df <- try_show(spark_collect(q)); cat("identical:", identical(df, base), "\n")

cat("## repeated response after reattach (same batch twice), with offsets\n")
srv <- new_server(list(list(B(1), B(2), transport_cnd()), list(B(2), B(3), B(4), RC())))
con <- srv$connect(); q <- new_q(con)
df <- try_show(spark_collect(q)); srv$show()
cat("## repeated response after reattach, no offsets\n")
srv <- new_server(list(list(nb(1), nb(2), transport_cnd()), list(nb(2), nb(3), nb(4), RC())))
con <- srv$connect(); q <- new_q(con)
df <- try_show(spark_collect(q)); if (is.data.frame(df)) print(df); srv$show()

cat("## response after ResultComplete\n")
srv <- new_server(list(list(B(1), B(2), B(3), RC(), B(4))))
con <- srv$connect(); q <- new_q(con)
df <- try_show(spark_collect(q)); if (is.data.frame(df)) print(nrow(df)); srv$show()

cat("## transport cut right after ResultComplete\n")
srv <- new_server(list(list(B(1), B(2), B(3), B(4), RC(), transport_cnd())))
con <- srv$connect(); q <- new_q(con)
df <- try_show(spark_collect(q)); cat("identical:", identical(df, base), "\n"); srv$show()

cat("## status error right after ResultComplete\n")
srv <- new_server(list(list(B(1), B(2), B(3), B(4), RC(), status_cnd("UNAVAILABLE"))))
con <- srv$connect(); q <- new_q(con)
df <- try_show(spark_collect(q)); cat("identical:", identical(df, base), "\n"); srv$show()

cat("## ResultComplete before anything\n")
srv <- new_server(list(list(RC())))
con <- srv$connect(); q <- new_q(con)
df <- try_show(spark_collect(q)); srv$show()
