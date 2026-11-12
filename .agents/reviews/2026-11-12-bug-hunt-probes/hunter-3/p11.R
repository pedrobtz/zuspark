source("pre.R"); library(DBI)
con <- spark_connect(URL); d <- dbConnect(spark_driver(), con = con)
sql <- "SELECT cast(id as int) id, cast(id as string) s, id/2 h FROM range(1000)"
q <- spark_sql(d, sql); a <- spark_collect(q); b <- as.data.frame(q); c <- dbGetQuery(d, sql); qc <- unserialize(serialize(q, NULL)); e <- spark_collect(qc)
say("identical a/b %s a/c %s a/e %s", identical(a, b), identical(a, c), identical(a, e))
st1 <- spark_fetch(q); st2 <- spark_fetch(q); invisible(gc())
say("stream1 after rerun+gc: %d ; stream2: %d", nrow(zuarrow::arrow_to_r(st1)), nrow(zuarrow::arrow_to_r(st2)))
# what travels with a saved query / error after a DBI query
say("serialized con size before big DBI query: %d", length(serialize(q, NULL)))
x <- dbGetQuery(d, "SELECT id, 'secret-row-data' AS s FROM range(200000)")
say("serialized query size after dbGetQuery of 200k rows: %d", length(serialize(q, NULL)))
err <- tryCatch(spark_sql(con, "SELECT * FROM nope_y"), error = function(e) e)
say("serialized error size: %d; contains row text: %s", length(serialize(err, NULL)), grepl("secret-row-data", rawToChar(serialize(err, NULL, ascii = TRUE)), fixed = TRUE))
say("fields on con: %s", paste(ls(con), collapse = " "))
rm(x); invisible(gc())
say("spark_disconnect(d) -> %s ; dbIsValid %s", spark_disconnect(d), dbIsValid(d))
say("data still on released con: %s rows", format(nrow(con$dbi_result$data)))
# token never travels
con <- spark_connect(URL, token = "TOPSECRETTOKEN123", headers = c("x-extra" = "HDRSECRET456"))
q <- spark_sql(con, "SELECT 1"); err <- tryCatch(spark_sql(con, "SELECT * FROM nope_y"), error = function(e) e)
for (o in list(con = con, q = q, err = err)) { s <- rawToChar(serialize(o, NULL, ascii = TRUE)); cat("token in serialized:", grepl("TOPSECRETTOKEN123", s, fixed = TRUE), " header:", grepl("HDRSECRET456", s, fixed = TRUE), "\n") }
print(con); cat(con$url, con$endpoint, "\n"); cat(capture.output(print(err$parent))[1:2], sep = "\n")
spark_disconnect(con); say("chan=%d", nchan())
