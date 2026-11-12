source("pre.R")
for (v in list(1e-9, 1e-3, Inf, 1e308, 1e10, Sys.time() - 60, 5L, as.difftime(5, units = "mins"))) {
  r <- tryCatch({ con <- spark_connect(URL, deadline = v); x <- spark_collect(spark_sql(con, "SELECT 1 AS x"))$x; d <- spark_disconnect(con); paste("ok", x, d) },
    error = function(e) paste("ERR", paste(head(class(e), 3), collapse = "/"), substr(conditionMessage(e), 1, 150)))
  cat(sprintf("%-28s -> %s  chan=%d\n", paste(class(v)[1], format(v)), r, nchan()))
}
# POSIXct deadline expiring during the connection's life
con <- spark_connect(URL, deadline = Sys.time() + 3); say("sid %s", con$session_id)
q <- spark_sql(con, "SELECT 1 AS x"); print(spark_collect(q)$x)
Sys.sleep(4)
tc(spark_config(con))
r <- tc(spark_collect(q)); print(class(r)); 
tc(spark_sql(con, "SELECT 2"))
print(tc(spark_disconnect(con))); print(con); say("chan=%d", nchan())
