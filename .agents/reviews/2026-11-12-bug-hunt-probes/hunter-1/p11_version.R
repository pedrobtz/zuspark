source("common.R")
P <- zuspark:::spark_pool()
hd <- function(con) get("headers", zuspark:::channels[[con$channel_key]])
mk <- function(ver, sid_fun = identity) function(con, method, bytes, deadline, streaming = FALSE) {
  req <- zuproto::proto_decode(bytes, sprintf("spark.connect.%sRequest", method), P)
  zuproto::proto_encode(c(list(session_id = sid_fun(req$session_id), server_side_session_id = "srv"), if (method == "AnalyzePlan") list(spark_version = list(version = ver))), sprintf("spark.connect.%sResponse", method), P)
}
options(zuspark.transport = mk("4.2.1"))
c5 <- spark_connect("sc://h.example?x=1", token = "t"); cat("token after '?': endpoint", c5$endpoint, " headers:", deparse(hd(c5)), " printed:", c5$url, "\n")
c6 <- spark_connect("sc://h.example/#frag", token = "t"); cat("token after '#': endpoint", c6$endpoint, " headers:", deparse(hd(c6)), "\n")
c7 <- spark_connect("sc://h.example", token = "t"); cat("control: endpoint", c7$endpoint, " headers:", deparse(hd(c7)), "\n")
for (v in c("3.5.0", "3.4.9", "3.10.0", "4.0.0-preview1", "v4.0.0", "", "4", "3.5", "10.0.0", "3.5.0.1", "4.1.0-SNAPSHOT", "abc", "3.5.0-amzn-0", " 4.0.0", "3.5.0.dev0", "4.0.0.")) {
  options(zuspark.transport = mk(v)); r <- t(spark_connect("sc://h"), FALSE)
  cat(sprintf("version '%s' -> %s\n", v, if (inherits(r, "err")) r[[2]] else paste("ok, spark_version() =", deparse(spark_version(r)))))
}
options(zuspark.transport = mk("4.0.0", toupper)); t(class(spark_connect("sc://h")))
options(zuspark.transport = mk("4.0.0", tolower)); t(class(spark_connect("sc://h", session_id = toupper(zuspark:::new_uuid()))))
options(zuspark.transport = mk("4.0.0"))
con <- spark_connect("sc://h"); print(spark_session_id(con)); t(spark_disconnect(con, allow_reconnect = TRUE)); print(spark_session_id(con))
# SPARK_REMOTE default and user env
Sys.setenv(SPARK_REMOTE = "sc://envhost:1/;user_id=envuser"); con <- spark_connect(); cat(con$url, con$user_id, "\n")
Sys.setenv(SPARK_USER = "sparkuser"); con <- spark_connect("sc://h"); cat("SPARK_USER ->", con$user_id, "\n"); con <- spark_connect("sc://h/;user_id=u2"); cat("url user ->", con$user_id, "\n")
Sys.setenv(SPARK_USER = "", USER = ""); con <- t(spark_connect("sc://h"), FALSE); cat("no env ->", con$user_id, "\n")
