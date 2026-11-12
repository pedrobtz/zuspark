source("pre.R")
mode <- commandArgs(TRUE)[1]
swap <- function(con) { key <- con$channel_key; ch <- get("channels", asNamespace("zuspark")); try(zugrpc::grpc_close(ch[[key]]), silent = TRUE); assign(key, zugrpc::grpc_channel("http://127.0.0.1:16099"), envir = ch) }
if (mode != "connect") { con <- spark_connect(URL); q <- spark_sql(con, "SELECT 1"); swap(con) }
cat("READY\n")
t0 <- Sys.time()
e <- tryCatch(switch(mode, connect = spark_connect("sc://127.0.0.1:16099"), config = spark_config(con), schema = spark_schema(q), disconnect = spark_disconnect(con), interrupt = spark_interrupt(con), collect = spark_collect(q)), condition = function(e) e)
cat(mode, "after", round(as.numeric(Sys.time() - t0, units = "secs"), 1), "s; class:", paste(class(e), collapse = "/"), "\n msg:", conditionMessage(e), "\n chan:", nchan(), "\n")
if (mode != "connect") cat(" released:", con$released, "\n")
