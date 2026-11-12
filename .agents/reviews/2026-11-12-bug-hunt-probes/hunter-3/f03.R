source("fake.R")
vals <- list(NA, NA_real_, 0, -1, 1e-300, 1e-9, 0.5, 5L, Inf, 1e308, "5", TRUE, c(1, 2), Sys.time() - 60, Sys.time() + 60, as.POSIXct(NA), as.difftime(5, units = "mins"), Sys.Date(), as.POSIXlt(Sys.time() + 60), NaN, numeric(0), list(5), bit64 = NULL)
for (v in vals) {
  reset()
  r <- tryCatch({ con <- spark_connect(FURL, deadline = v); spark_config(con); spark_sql(con, "SELECT 1"); spark_disconnect(con)
    paste(unique(vapply(log$calls, function(c) paste(class(c$deadline)[1], format(c$deadline)), "")), collapse = " | ") },
    error = function(e) paste("ERR", class(e)[1]))
  cat(sprintf("%-28s -> %s\n", paste(class(v)[1], paste(format(v), collapse = ",")), r))
}
say("chan %d", nchan())
