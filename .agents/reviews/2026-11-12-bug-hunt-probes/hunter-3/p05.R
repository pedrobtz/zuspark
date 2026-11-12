source("pre.R")
# gc of original while unserialized copy still in use
mk <- function() { con <- spark_connect(URL); q <- spark_sql(con, "SELECT id FROM range(3)"); unserialize(serialize(q, NULL)) }
q2 <- mk(); say("F sid %s chan=%d", q2$con$session_id, nchan())
invisible(gc()); invisible(gc())
say("after gc chan=%d", nchan())
r <- tc(spark_collect(q2)); print(q2$con)
# finalizer of A while B reads a stream
B <- spark_connect(URL)
local({ A <- spark_connect(URL); say("G A sid %s", A$session_id); NULL })
qb <- spark_sql(B, "SELECT id, repeat('x', 100) s FROM range(300000)")
df <- spark_collect(qb); say("B rows %d reatt %d chan=%d", nrow(df), qb$reattaches, nchan())
invisible(gc()); say("chan=%d", nchan())
say("B ok: %s", identical(spark_collect(spark_sql(B, "SELECT 1 AS x"))$x, 1L))
# exit without disconnect: does session stay?
say("H leaving session %s open at exit", B$session_id)
