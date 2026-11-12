source("pre.R")
mode <- commandArgs(TRUE)[1]
con <- spark_connect(URL)
cat("SID", con$session_id, "\n")
q <- spark_sql(con, "SELECT sum(id % 7) AS s FROM range(300000000000)")
cat("READY\n"); flush(stdout())
t0 <- Sys.time()
e <- tryCatch(
  if (mode == "collect") spark_collect(q) else if (mode == "dbi") { d <- DBI::dbConnect(spark_driver(), con = con); DBI::dbGetQuery(d, "SELECT sum(id % 7) AS s FROM range(300000000000)") } else spark_execute(con, "SELECT sum(id % 7) AS s FROM range(300000000000)"),
  condition = function(e) e)
cat("after", round(as.numeric(Sys.time() - t0, units = "secs"), 1), "s; class:", paste(class(e), collapse = "/"), "\n")
cat("msg:", conditionMessage(e), "\n")
cat("op:", q$operation_id, " err op:", e$operation_id, " ids:", e$interrupted_ids, "\n")
cat("q rows:", format(q$rows), "\n")
r <- tryCatch(spark_collect(spark_sql(con, "SELECT 5 AS x"))$x, condition = function(e) conditionMessage(e))
cat("usable after:", r, "\n")
if (mode == "dbi") { cat("dbi_result cleared:", con$dbi_result$cleared, " valid:", DBI::dbIsValid(d), "\n") }
cat("disc:", tryCatch(spark_disconnect(con), condition = function(e) conditionMessage(e)), " chan", nchan(), "\n")
