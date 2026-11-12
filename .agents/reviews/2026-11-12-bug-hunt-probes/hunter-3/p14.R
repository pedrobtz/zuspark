source("pre.R")
swap <- function(con) { key <- con$channel_key; ch <- get("channels", asNamespace("zuspark")); try(zugrpc::grpc_close(ch[[key]]), silent = TRUE); assign(key, zugrpc::grpc_channel("http://127.0.0.1:16099"), envir = ch) }
# finalizer against a server that stops answering
local({ con <- spark_connect(URL); swap(con); NULL })
t <- system.time(invisible(gc()))[["elapsed"]]; say("gc with dead server took %.1f s; chan=%d", t, nchan())
# two dead connections
local({ a <- spark_connect(URL); swap(a); b <- spark_connect(URL); swap(b); NULL })
t <- system.time(invisible(gc()))[["elapsed"]]; say("gc with two dead took %.1f s; chan=%d", t, nchan())
# disconnect with deadline 2 on dead server
con <- spark_connect(URL, deadline = 2); swap(con)
t <- system.time(r <- tc(spark_disconnect(con)))[["elapsed"]]; say("disconnect took %.1f s released=%s chan=%d", t, con$released, nchan())
# query on dead server, deadline 2: stream has no deadline -> would hang; test unary only
con <- spark_connect(URL, deadline = 2); swap(con)
t <- system.time(r <- tc(spark_config(con)))[["elapsed"]]; say("config took %.1f s", t)
t <- system.time(local({ rm(con, envir = globalenv()); invisible(gc()) }))[["elapsed"]]; say("finaliser with con deadline 2 took %.1f s (uses 5)", t)
