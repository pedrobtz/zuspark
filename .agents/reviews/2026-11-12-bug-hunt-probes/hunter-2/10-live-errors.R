source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/hunt-2/live.R")
con <- spark_connect(MAIN)
for (s in c("SELECT java_method('java.lang.Integer','parseInt','x')", "SELECT sum(sum(id)) FROM range(3)", "SELECT 1 FROM range(2) a NATURAL CROSS JOIN range(2) b", "SELECT * FROM range(1) LIMIT (SELECT 1)", "WITH t AS (SELECT 1) SELECT * FROM t t1 JOIN LATERAL (SELECT 1) ON true ON true", "SELECT a.* FROM range(1)", "SELECT 1 UNION SELECT 1, 2", "SELECT * FROM (SELECT 1) PIVOT (sum(1) FOR x IN (1))", "SET spark.sql.shuffle.partitions = -1", "SELECT ? ", "EXECUTE IMMEDIATE 'SELECT 1' INTO nosuchvar", "DROP TABLE nonexist_t", "SELECT nosuchfn(1)",
            "SELECT * FROM range(1) WHERE", "CREATE TABLE t_dup (a INT) USING nosuchformat", "SELECT element_at(array(1),5)", "SELECT e'\\x'")) {
  cat("##", s, "\n")
  e <- tryCatch({ x <- spark_sql(con, s); if (inherits(x, "zuspark_query")) spark_collect(x) else x }, error = function(e) e)
  if (inherits(e, "condition")) { cat("  ", class(e)[1], "| class:", e$error_class, "| state:", e$sql_state, "\n   params: "); print(e$message_parameters); pe <- e$parent; cat("   raw messageParameters:", tryCatch(pe$details$details$ErrorInfo$metadata[["messageParameters"]], error = function(e) "<absent>"), "\n") } else print(e)
}
spark_disconnect(con)
