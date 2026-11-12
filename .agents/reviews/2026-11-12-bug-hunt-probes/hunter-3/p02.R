source("pre.R")
con <- spark_connect(URL)
err <- tc(spark_sql(con, "SELECT * FROM nope_table"))
for (i in 1:3) { d <- tc(spark_error_details(err)); cat(i, class(d)[1], length(d), "\n") }
err <- tc(spark_sql(con, "SELECT * FROM nope_table2"))
Sys.sleep(1)
for (i in 1:3) { d <- tc(spark_error_details(err)); cat(i, class(d)[1], length(d), "\n") }
err2 <- unserialize(serialize(err, NULL))
cat("same con?", identical(err$con, con), identical(err2$con, con), "\n")
spark_disconnect(con)
