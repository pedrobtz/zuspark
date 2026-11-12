source("pre.R"); library(DBI)
w <- function(expr) withCallingHandlers(tryCatch(expr, error = function(e) { cat("  <ERR ", class(e)[1], "> ", substr(conditionMessage(e), 1, 110), "\n", sep = ""); invisible(e) }), warning = function(w) { cat("  <WARN> ", conditionMessage(w), "\n", sep = ""); invokeRestart("muffleWarning") })
d <- dbConnect(spark_driver(), URL)
r1 <- dbSendQuery(d, "SELECT id FROM range(10)")
say("fetch 3: %d rows; completed %s; rowcount %d", nrow(dbFetch(r1, 3)), dbHasCompleted(r1), dbGetRowCount(r1))
r2 <- w(dbSendQuery(d, "SELECT id FROM range(4)"))   # second open result
say("r1 valid %s r2 valid %s", dbIsValid(r1), dbIsValid(r2))
w(dbFetch(r1)); 
say("r2 rows %d", nrow(dbFetch(r2)))
w(dbClearResult(r2)); w(dbClearResult(r2)); w(dbFetch(r2)); w(dbHasCompleted(r2)); w(dbGetRowCount(r2)); w(dbColumnInfo(r2)); w(dbGetStatement(r2))
say("data retained on con after clear: %s", !is.null(d@con$dbi_result$data))
# failing dbSendQuery closes the open one?
r3 <- dbSendQuery(d, "SELECT 1 AS x")
e <- w(dbSendQuery(d, "SELECT * FROM nope_zz"))
say("r3 valid after failed send: %s", dbIsValid(r3)); w(print(dbFetch(r3)))
# two DBI wrappers of one connection
d2 <- dbConnect(spark_driver(), con = d@con)
ra <- dbSendQuery(d, "SELECT 1 AS a"); rb <- w(dbSendQuery(d2, "SELECT 2 AS b"))
say("ra valid %s", dbIsValid(ra)); w(dbFetch(ra)); print(dbFetch(rb)); dbClearResult(rb)
# result then spark_* verbs on same con (no warning?), fetch later
r4 <- dbSendQuery(d, "SELECT id FROM range(3)")
x <- w(dbGetQuery(d, "SELECT 9 AS z")); print(x)
say("r4 valid after dbGetQuery: %s", dbIsValid(r4))
r5 <- dbSendQuery(d, "SELECT id FROM range(3)")
x <- w(dbExecute(d, "SELECT 1")); x <- w(dbListTables(d)); x <- w(dbExistsTable(d, "nonex"))
say("r5 valid after dbExecute/dbListTables/dbExistsTable: %s rows %d", dbIsValid(r5), nrow(dbFetch(r5)))
# disconnect with open result
r6 <- dbSendQuery(d, "SELECT id FROM range(3)"); r7data <- NULL
print(w(dbDisconnect(d)))
say("after disconnect: con valid %s, r6 valid %s, d2 valid %s", dbIsValid(d), dbIsValid(r6), dbIsValid(d2))
w(dbFetch(r6)); w(dbHasCompleted(r6)); w(dbColumnInfo(r6)); print(w(dbClearResult(r6)))
print(w(dbDisconnect(d))); print(w(dbDisconnect(d2)))
w(dbGetQuery(d, "SELECT 1")); w(dbSendQuery(d, "SELECT 1")); w(dbListTables(d)); print(w(dbGetInfo(d))$db.version); w(print(d))
# result fetched fully then disconnect: fetch again
d <- dbConnect(spark_driver(), URL)
r <- dbSendQuery(d, "SELECT id FROM range(5)"); a <- dbFetch(r, 2); dbDisconnect(d)
say("fetch rest after disconnect: %d rows (valid %s)", nrow(w(dbFetch(r))), dbIsValid(r))
# a result from one connection used after another connection is made
d1 <- dbConnect(spark_driver(), URL); r <- dbSendQuery(d1, "SELECT 1 AS one"); dd <- dbConnect(spark_driver(), URL); rr <- dbSendQuery(dd, "SELECT 2 AS two")
print(dbFetch(r)); print(dbFetch(rr)); dbClearResult(r); dbClearResult(rr); dbDisconnect(d1); dbDisconnect(dd)
# equivalence spark_connect vs dbConnect; collect vs as.data.frame vs dbGetQuery
con <- spark_connect(URL); d <- dbConnect(spark_driver(), con = con)
sql <- "SELECT id, cast(id as string) s, id/2.0 h FROM range(1000)"
q <- spark_sql(d, sql); a <- spark_collect(q); b <- as.data.frame(q); c <- w(dbGetQuery(d, sql, lossy = "error")); qc <- unserialize(serialize(q, NULL)); e <- spark_collect(qc)
say("identical a/b %s a/c %s a/e %s", identical(a, b), identical(a, c), identical(a, e)); if (is.data.frame(c)) print(all.equal(a, c))
st1 <- spark_fetch(q); st2 <- spark_fetch(q); invisible(gc())
say("stream1 after rerun+gc: %d ; stream2: %d", nrow(zuarrow::arrow_to_r(st1)), nrow(zuarrow::arrow_to_r(st2)))
say("spark_disconnect(d) -> %s ; dbIsValid %s", spark_disconnect(d), dbIsValid(d))
say("chan=%d", nchan())
