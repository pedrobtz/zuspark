source("pre.R")
con <- spark_connect("sc://localhost:16022", token = "TOPSECRETTOKEN123", headers = c("x-extra" = "HDRSECRET456"))
q <- spark_sql(con, "SELECT 1"); err <- tryCatch(spark_sql(con, "SELECT * FROM nope_y"), error = function(e) e)
ierr <- tryCatch(spark_config(con, "no.such.key.zz"), error = function(e) e); print(class(ierr))
for (o in list(con = con, q = q, err = err, ierr = ierr)) { s <- rawToChar(serialize(o, NULL, ascii = TRUE)); cat("token in serialized:", grepl("TOPSECRETTOKEN123", s, fixed = TRUE), " header:", grepl("HDRSECRET456", s, fixed = TRUE), "\n") }
print(con); cat(con$url, con$endpoint, "\n")
spark_disconnect(con); say("chan=%d", nchan())
