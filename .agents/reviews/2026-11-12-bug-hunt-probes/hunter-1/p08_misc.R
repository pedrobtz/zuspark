source("common.R")
library(DBI)
mode <- commandArgs(TRUE)[[1]]
cat("#### mode:", mode, Sys.getlocale("LC_CTYPE"), "\n")
con <- dbConnect(spark_driver(), URL)
spark_config(con, spark.sql.session.timeZone = "UTC")
if (mode == "C") {
  s <- "zo\u00eb"; cat(Encoding(s), "\n")
  r <- t(dbGetQuery(con, paste0("SELECT ", dbQuoteString(con, s), " AS x, length(", dbQuoteString(con, s), ") AS n")), FALSE)
  if (!inherits(r, "err")) { cat("out bytes:", paste(charToRaw(r$x), collapse = ""), " n:", r$n, " enc:", Encoding(r$x), " same:", identical(charToRaw(r$x), charToRaw(s)), "\n") }
  l1 <- "caf\xe9"; Encoding(l1) <- "latin1"
  r <- t(dbGetQuery(con, paste0("SELECT ", dbQuoteString(con, l1), " AS x")), FALSE); if (!inherits(r, "err")) cat("latin1 out bytes:", paste(charToRaw(r$x), collapse = ""), Encoding(r$x), "\n")
  t({spark_config(con, zz.c = s); x <- spark_config(con, "zz.c"); c(paste(charToRaw(x), collapse = ""), Encoding(x))})
  t({c2 <- spark_connect(URL, user_id = s); spark_disconnect(c2)})
  t({c2 <- spark_connect(paste0(URL, "/;user_id=zo%C3%AB")); spark_disconnect(c2)})
  t({c2 <- spark_connect(URL, "user_id" = s, user_agent = "x"); spark_disconnect(c2)})
  t(zuspark:::append_url_params("sc://h", list(user_id = s)))
  t(zuspark:::parse_json_strings("{\"a\":\"zo\u00eb \\u00e9\",\"b\":\"x\"}"))
  t(zuspark:::parse_json_strings(rawToChar(as.raw(c(0x5b, 0x22, 0x7a, 0xc3, 0xab, 0x22, 0x5d)))))
  q <- t(spark_sql(con, paste0("SELECT '", strrep(s, 80), "' AS x")), FALSE); t(print(q))
  e <- tryCatch(spark_sql(con, paste0("SELECT * FROM `no_", s, "`")), error = identity); t(class(e)); t(e$message_parameters)
  t(zuspark:::client_type(s))
  t(spark_schema(spark_sql(con, paste0("SELECT 1 AS `", s, "`"))))
}
if (mode == "numeric") {
  suppressWarnings(Sys.setlocale("LC_NUMERIC", "de_DE.UTF-8")); cat(Sys.getlocale("LC_NUMERIC"), sprintf("%.1f", 0.5), format(0.5), "\n")
  t(as.character(dbQuoteLiteral(con, c(0.5, 1234567.891))))
  t(dbGetQuery(con, paste("SELECT", dbQuoteLiteral(con, 0.5), "AS x")))
  t(zuspark:::scalar_text(0.5)); t(as.character(dbQuoteLiteral(con, as.POSIXct(1.5, origin = "1970-01-01"))))
}
if (mode == "nobit64") {
  cat("bit64 loaded:", isNamespaceLoaded("bit64"), "\n")
  x <- structure(c(4.94065645841247e-324, 2.47032822920623e-323), class = "integer64")  # 1, 5
  t(dbDataType(con, x)); t(as.character(dbQuoteLiteral(con, x)))
  t(dbGetQuery(con, paste("SELECT", dbQuoteLiteral(con, x)[2], "AS x"), lossy = "allow"))
  t(zuspark:::scalar_text(x[2]))
}
if (mode == "dbplyr") {
  suppressMessages(library(dplyr))
  df <- data.frame(s = c("it's", "back\\slash", "a", "zo\u00eb", "${x}", "100%", NA), n = c(1.5, 2, 1e5, 0.1, NA, 3, 4), stringsAsFactors = FALSE)
  t({tb <- copy_to(con, df, "zzdb", temporary = TRUE, overwrite = TRUE); r <- collect(tb); print(as.data.frame(r)); identical(r$s, df$s)})
  tb <- tbl(con, "zzdb")
  for (v in df$s[1:6]) { r <- t(collect(filter(tb, s == !!v)), FALSE); cat(sprintf("filter s == %-12s -> %s rows\n", v, if (inherits(r, "err")) "ERR" else nrow(r))) }
  t(show_query(filter(tb, s == "it's", n > 1e5, n == 0.1)))
  t(as.data.frame(collect(filter(tb, n == 0.1))))
  t(as.data.frame(collect(filter(tb, n >= 100000))))
  t(as.data.frame(collect(mutate(tb, d = as.Date("2024-01-02"), t = !!as.POSIXct("2024-01-02 03:04:05", tz = "America/New_York"), i = 100000L, b = TRUE) |> select(d, t, i, b) |> head(1))))
  t(as.data.frame(collect(filter(tb, s %in% c("it's", "a")))))
  t(print(tb))
  t(dbplyr::remote_name(tb)); t(as.data.frame(collect(count(tb))))
  t({tb2 <- compute(filter(tb, n > 1), "zzcomp", temporary = FALSE, overwrite = TRUE); nrow(collect(tb2))})
  t({tb2 <- compute(filter(tb, n > 2), "zzcomp", temporary = FALSE, overwrite = TRUE); nrow(collect(tb2))})
  t(dbRemoveTable(con, "zzcomp"))
  t({copy_to(con, data.frame(f = factor(c("a", "b")), d = Sys.Date(), t = as.POSIXct("2024-01-02 03:04:05", tz = "UTC")), "zzcp2", temporary = TRUE, overwrite = TRUE) |> collect() |> as.data.frame()})
}
dbDisconnect(con)
