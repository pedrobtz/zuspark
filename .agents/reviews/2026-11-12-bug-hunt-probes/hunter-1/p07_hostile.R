source("common.R")
library(DBI)
args <- commandArgs(TRUE)
mode <- if (length(args)) args[[1]] else "plain"
cat("#### mode:", mode, " locale:", Sys.getlocale("LC_CTYPE"), " TZ:", Sys.getenv("TZ"), "\n")
if (mode == "opts") options(OutDec = ",", scipen = -5, digits = 3, stringsAsFactors = TRUE)
if (mode == "numeric") print(tryCatch(Sys.setlocale("LC_NUMERIC", "de_DE.UTF-8"), warning = function(w) conditionMessage(w)))
con <- dbConnect(spark_driver(), URL)
spark_config(con, spark.sql.session.timeZone = "UTC")
df <- data.frame(
  i = c(1L, 100000L, 123456L, NA),
  d = c(0.5, 1e5, 1234567.891, 1e-5),
  s = c("a", "zoë", "it's", NA),
  dt = as.Date(c("2024-03-10", "1970-01-01", "2024-11-03", NA)),
  ts = as.POSIXct(c("2024-03-10 02:30:00", "1970-01-01 00:00:00", "2024-11-03 01:30:00", NA), tz = "UTC"),
  b = c(TRUE, FALSE, NA, TRUE),
  stringsAsFactors = FALSE
)
df$ts_local <- as.POSIXct(c(1710037800, 0, 1730597400, NA))  # no tzone attribute: session TZ
lits <- lapply(df, function(x) as.character(dbQuoteLiteral(con, x)))
str(lits)
t({dbWriteTable(con, "zzh", df, temporary = TRUE, overwrite = TRUE)
  r <- dbGetQuery(con, "SELECT i, d, s, CAST(dt AS STRING) dt, CAST(ts AS STRING) ts, unix_micros(ts) us, unix_micros(ts_local) us_local, b FROM zzh")
  print(r)
  cat("i same:", identical(r$i, df$i), " d same:", isTRUE(all.equal(r$d, df$d, tolerance = 0)), " s same:", identical(enc2utf8(r$s), enc2utf8(df$s)),
      " ts same:", identical(r$us / 1e6, as.numeric(df$ts)), " ts_local same:", identical(r$us_local / 1e6, as.numeric(df$ts_local)), "\n")
  rr <- dbReadTable(con, "zzh"); str(rr); cat("readback ts equal:", isTRUE(all.equal(as.numeric(rr$ts), as.numeric(df$ts))), " dt equal:", identical(as.numeric(rr$dt), as.numeric(df$dt)), "\n")
}, FALSE)
t(spark_config(con, zz.h = 0.5)); t(spark_config(con, "zz.h"))
t(zuspark:::append_url_params("sc://h", list(grpc_max_message_size = 1e6, x = 0.25)))
q <- spark_sql(con, "SELECT 'zoë' AS s, id FROM range(1234567)"); invisible(spark_collect(q)); print(q)
t(spark_schema(spark_sql(con, "SELECT 1 AS `zoë`, CAST(1 AS DECIMAL(20,5)) d")))
t(zuspark:::parse_json_strings("{\"a\":\"zoë \\u00e9 \\ud83d\\ude00\",\"b\":\"x\"}"))
t(zuspark:::client_type("zoë"))
e <- tryCatch(spark_sql(con, "SELECT * FROM `no_such_zoë`"), error = identity); t(e$message_parameters); t(e$error_class); t(e$sql_state); t(conditionMessage(e))
t({d <- spark_error_details(e); list(length(d), d[[1]]$error_class, d[[1]]$message_parameters, d[[1]]$cause, head(d[[1]]$stack, 2), d[[1]]$contexts)})
dbDisconnect(con)
