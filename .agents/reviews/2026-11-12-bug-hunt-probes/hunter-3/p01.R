source("pre.R")
con <- spark_connect(URL)
say("chan %d", nchan())
q <- spark_sql(con, "SELECT id FROM range(5)")
st <- spark_fetch(q)
df1 <- spark_collect(q)
print(q$operation_id); print(q$rows)
# copy via serialize in same session
con2 <- unserialize(serialize(con, NULL))
say("copy identical env? %s; released %s", identical(con, con2), con2$released)
q2 <- unserialize(serialize(q, NULL))
print(tc(spark_collect(q2)))
# disconnect twice
print(spark_disconnect(con)); print(spark_disconnect(con))
say("chan %d", nchan())
# stream read after disconnect
print(tc(zuarrow::arrow_to_r(st)))
print(tc(zuarrow::arrow_to_r(st)))  # twice
# query after disconnect
tc(spark_collect(q)); print(q)
# copy after original disconnected
print(con2); tc(spark_collect(q2)); tc(spark_session_id(con2)); tc(spark_disconnect(con2)); print(con2)
print(spark_session_id(con)); print(spark_version(con)); print(con)
tc(spark_interrupt(con)); tc(spark_config(con)); tc(spark_sql(con, "select 1"))
# sql error + details after disconnect
con <- spark_connect(URL)
err <- tc(spark_sql(con, "SELECT * FROM nope_table"))
str(names(err))
d <- tc(spark_error_details(err)); cat(length(d), "\n")
f <- tempfile(); saveRDS(err, f); err2 <- readRDS(f)
d2 <- tc(spark_error_details(err2)); cat(length(d2), "\n")
spark_disconnect(con)
tc(spark_error_details(err)); tc(spark_error_details(err2))
saveRDS(err, "err.rds")
