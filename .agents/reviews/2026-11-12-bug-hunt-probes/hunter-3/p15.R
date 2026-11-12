source("pre.R")
swap <- function(con) { key <- con$channel_key; ch <- get("channels", asNamespace("zuspark")); try(zugrpc::grpc_close(ch[[key]]), silent = TRUE); assign(key, zugrpc::grpc_channel("http://127.0.0.1:16099"), envir = ch) }
con <- spark_connect(URL); swap(con)
t <- system.time(r <- tc(zuspark:::release_session(con, deadline = 5)))[["elapsed"]]; say("1st release took %.1f s", t)
t <- system.time(r <- tc(zuspark:::release_session(con, deadline = 5)))[["elapsed"]]; say("2nd release took %.1f s", t)
con2 <- spark_connect(URL); swap(con2)
t <- system.time(r <- tc(spark_config(con2)))[["elapsed"]]; say("config took %.1f s", t)
