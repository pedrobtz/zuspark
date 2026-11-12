source("pre.R")
mk <- function() { con <- spark_connect(URL); q <- spark_sql(con, "SELECT id, repeat('x', 200) s FROM range(400000)"); unserialize(serialize(q, NULL)) }
q2 <- mk(); say("sid %s chan=%d", q2$con$session_id, nchan())
r <- tryCatch(spark_collect(q2), error = function(e) e)
if (is.data.frame(r)) say("rows %d sum ok %s; chan=%d", nrow(r), sum(r$id) == sum(0:399999), nchan()) else { say("ERR %s: %s", class(r)[1], substr(conditionMessage(r), 1, 200)); say("chan=%d; q2 rows=%s", nchan(), format(q2$rows)) }
r <- tc(spark_collect(q2))
# POSIXct deadline passed: do releases reach the server?
con <- spark_connect(URL, deadline = Sys.time() + 2); Sys.sleep(2.5)
q <- spark_sql(con, "SELECT 1 AS x"); print(spark_collect(q)$x); say("POSIX sid %s op %s", con$session_id, q$operation_id)
tc(spark_disconnect(con))
