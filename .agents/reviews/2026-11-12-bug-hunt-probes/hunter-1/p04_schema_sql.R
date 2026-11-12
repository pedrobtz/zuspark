source("common.R")
options(zuspark.transport = spy_transport)
con <- spark_connect(URL)
cat("## schema vs typeof\n")
exprs <- c("CAST(1 AS DECIMAL(38,18))", "CAST(1 AS DECIMAL(1,0))", "CAST(1 AS DECIMAL)", "CAST(1 AS DECIMAL(10,-0))",
 "array(1,2)", "array(array('a'))", "map('a', 1)", "map(1, array(named_struct('x', 1)))", "named_struct('a', 1, 'b', 'x')",
 "named_struct('a b', 1, 'c:d', 2, 'e,f', named_struct('`q`', 1Y))", "named_struct('', 1)",
 "CAST('a' AS CHAR(5))", "CAST('a' AS VARCHAR(7))", "'a' COLLATE UNICODE_CI", "'a' COLLATE UTF8_LCASE", "'a' COLLATE UTF8_BINARY",
 "array('a' COLLATE UNICODE)", 
 "INTERVAL '1' YEAR", "INTERVAL '1' MONTH", "INTERVAL '1-2' YEAR TO MONTH", "INTERVAL '1' DAY", "INTERVAL '1' HOUR", "INTERVAL '1' MINUTE", "INTERVAL '1' SECOND",
 "INTERVAL '1 2' DAY TO HOUR", "INTERVAL '1 2:3' DAY TO MINUTE", "INTERVAL '1 2:3:4' DAY TO SECOND", "INTERVAL '2:3' HOUR TO MINUTE", "INTERVAL '2:3:4' HOUR TO SECOND", "INTERVAL '3:4' MINUTE TO SECOND",
 "make_interval(1,2,3,4,5,6,7.5)", "NULL", "X'00'", "1Y", "1S", "1L", "1.5F", "1.5D", "true", "DATE'2020-01-01'", "TIMESTAMP'2020-01-01 00:00:00'", "TIMESTAMP_NTZ'2020-01-01 00:00:00'",
 "parse_json('1')", "TIME'12:00:00'", "CAST(TIME'12:00:00' AS TIME(3))", "CAST(TIME'12:00:00' AS TIME(0))", "array()", "map()", "struct(1, 2)", "current_user()")
for (e in exprs) {
  r <- t({q <- spark_sql(con, sprintf("SELECT typeof(c) AS ty FROM (SELECT %s AS c)", e)); ty <- spark_collect(q)$ty; s <- spark_schema(spark_sql(con, sprintf("SELECT %s AS c", e))); c(typeof = ty, schema = s$type, nullable = s$nullable)}, FALSE)
  if (!inherits(r, "err")) cat(sprintf("%-6s %-45s typeof=%s | schema=%s\n", if (r[[1]] == r[[2]]) "ok" else "DIFF", e, r[[1]], r[[2]]))
}
cat("## schema names\n")
t(spark_schema(spark_sql(con, "SELECT 1 AS `a b`, 2 AS `é`, 3 AS a, 4 AS a, 5 AS ``, CAST(NULL AS INT) AS n, 1 AS `x\ny`")))
t(spark_schema(spark_sql(con, "SELECT * FROM range(0) WHERE 1=0")))
t(spark_schema("select 1")); t(spark_schema(con)); t(spark_schema(data.frame()))
cat("## spark_sql args\n")
l1 <- "SELECT 'caf\xe9' AS x"; Encoding(l1) <- "latin1"
for (s in list(l1, "SELECT 'zoë' AS x", structure("SELECT 'g' AS x", class = c("glue","character")), c(a = "SELECT 'named' AS x"), "  SELECT 'ws' AS x  \n", "SELECT 'a\nb' AS x", "SELECT 'semi' AS x;", "SELECT '\U0001F600' AS x")) {
  r <- t(spark_collect(spark_sql(con, s))$x, FALSE); cat(deparse(s), "->", deparse(r), Encoding(r), "\n")
}
t(spark_collect(spark_sql(con, DBI::SQL("SELECT 'SQL' AS x"))))
for (s in list(NA, NULL, "", "  ", c("select 1","select 2"), 1, factor("select 1"), list("select 1"), quote(x), TRUE)) { cat(deparse(s), ": "); t(spark_sql(con, s)) }
t(spark_sql("select 1")); t(spark_sql(con)); 
cat("## print query\n")
q <- spark_sql(con, "SELECT   id\n\tFROM range(100000)"); print(q); invisible(spark_collect(q)); print(q); str(q$rows)
q2 <- spark_sql(con, "SELECT id FROM range(123456)"); invisible(spark_collect(q2)); print(q2)
q3 <- spark_sql(con, paste0("SELECT '", strrep("é", 80), "' AS x")); print(q3)
q4 <- spark_sql(con, "SELECT 1 WHERE 1=0"); invisible(spark_collect(q4)); print(q4)
options(scipen = -5, digits = 3); print(q2); options(scipen = 0, digits = 7)
t(spark_execute(con, "SELECT id FROM range(100000)")); r <- spark_execute(con, "SELECT id FROM range(100000)"); str(r)
t({x <- spark_execute(con, "SHOW DATABASES"); x}); 
t(spark_execute(con, "CREATE OR REPLACE TEMPORARY VIEW zzv AS SELECT 1 AS a"))
cat("## collect args\n")
q <- spark_sql(con, "SELECT 1L AS a, CAST(1.5 AS DECIMAL(5,2)) AS d")
t(spark_collect(q)); t(spark_collect(q, lossy = "allow")); t(spark_collect(q, lossy = NA)); t(spark_collect(q, lossy = c("allow","error"))); t(spark_collect(q, lossy = "ALLOW")); t(spark_collect(q, lossy="allow", to = "x")); t(as.data.frame(q, lossy = "allow")); t(as.data.frame(q))
t(spark_collect(con)); t(spark_fetch("x"))
cat("## metrics / rows after steps\n")
q <- spark_sql(con, "SELECT id FROM range(10)"); str(q$rows); invisible(spark_collect(q)); str(q$rows); s <- spark_fetch(q); str(q$rows); str(names(q$metrics)); 
cat("## deprecated conf warn=2\n")
t(spark_config(con, spark.sql.adaptive.shuffle.targetPostShuffleInputSize = "1000"))
options(warn = 2)
t(spark_config(con, spark.sql.adaptive.shuffle.targetPostShuffleInputSize = "2000", zz.unset.me = NULL)); options(warn = 0)
t(spark_config(con, "spark.sql.adaptive.shuffle.targetPostShuffleInputSize"))
spark_disconnect(con)
print(q); t(spark_collect(q)); t(spark_schema(q))
