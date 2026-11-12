source("common.R")
options(zuspark.transport = spy_transport)
cat("## session_id variants\n")
u <- zuid::uuid(version = 4); print(class(u))
t({con <- spark_connect(URL, session_id = u); print(spark_session_id(con)); spark_disconnect(con)})
U <- toupper(as.character(zuid::uuid(version=4)))
t({con <- spark_connect(URL, session_id = U); print(spark_session_id(con)); print(spark_sql(con, "select 1 as a") |> spark_collect()); spark_disconnect(con)})
t({con <- spark_connect(paste0(URL, "/;session_id=", U)); print(spark_session_id(con)); spark_disconnect(con)})
t(spark_connect(URL, session_id = c(U, U)))
t(spark_connect(URL, session_id = NA))
t(spark_connect(URL, session_id = 5))
cat("## user_id routes\n")
l1 <- "caf\xe9"; Encoding(l1) <- "latin1"
for (v in list("zoë", l1, "a b;c=d", "x\ny")) {
  r1 <- t({con <- spark_connect(URL, user_id = v); x <- last_req("AnalyzePlan")$user_context$user_id; spark_disconnect(con); x}, FALSE)
  r2 <- t({con <- spark_connect(URL, "user_id" = v, user_agent="x"); 1}, FALSE)
  r3 <- t({con <- do.call(spark_connect, list(paste0(URL, "/;user_id=", utils::URLencode(enc2utf8(v), reserved=TRUE)))); x <- last_req("AnalyzePlan")$user_context$user_id; spark_disconnect(con); x}, FALSE)
  cat(sprintf("arg: %s [%s] url: %s [%s] identical=%s\n", r1, paste(charToRaw(r1),collapse=""), r3, Encoding(r3), identical(charToRaw(r1), charToRaw(r3))))
}
t(spark_connect(URL, user_id = ""))
t({con <- spark_connect(paste0(URL, "/;user_id=")); x <- last_req("AnalyzePlan")$user_context; spark_disconnect(con); x})
t({con <- spark_connect(paste0(URL, "/;user_agent=")); x <- last_req("AnalyzePlan")$client_type; spark_disconnect(con); x})
t({con <- spark_connect(URL, user_agent = "ag ent/1 é"); x <- last_req("AnalyzePlan")$client_type; spark_disconnect(con); x})
t({con <- spark_connect(paste0(URL, "/;user_agent=fromurl"), user_agent = "fromarg"); x <- last_req("AnalyzePlan")$client_type; spark_disconnect(con); x})
t({con <- spark_connect(paste0(URL, "/;user_id=fromurl"), user_id = "fromarg"); x <- last_req("AnalyzePlan")$user_context$user_id; spark_disconnect(con); x})
Sys.setenv(SPARK_CONNECT_USER_AGENT = "")
t({con <- spark_connect(URL); x <- last_req("AnalyzePlan")$client_type; spark_disconnect(con); x})
Sys.unsetenv("SPARK_CONNECT_USER_AGENT")
t(spark_connect(URL, user_agent = strrep("é", 400)))
cat("## deadline\n")
for (d in list(NA, -1, 0, 0.5, 1e-9, Inf, .Machine$integer.max + 1, 1e300, "5", c(1,2), TRUE, 5L, Sys.time() + 5, Sys.time() - 5, as.difftime(5, units="secs"), Sys.Date()+1, NA_real_, NaN)) {
  cat("deadline =", format(d), class(d), ": ")
  t({con <- spark_connect(URL, deadline = d); x <- spy$calls[[length(spy$calls)]]$deadline; r <- names(spark_config(con, "spark.app.name")); spark_disconnect(con); "ok"})
}
cat("## headers\n")
for (h in list(c(a="1"), list(x=c("a","b")), c("X-Upper"="1"), c(a=NA), c("a b"="1"), c(a="é"), c(a="x\ny"), c(a="1", a="2"), "noname", c(a=1), list(a=1), list(a="1", b=2L), character(), setNames("v", NA), c(authorization="Bearer zz"), c(":path"="x"), c("content-type"="text/plain"), c("te"="x"), c("grpc-timeout"="1n"), c("user-agent"="zz"))) {
  cat("headers =", deparse(h), ": ")
  t({con <- spark_connect(URL, headers = h); spark_disconnect(con); "ok"})
}
cat("## url header params\n")
t({con <- spark_connect(paste0(URL, "/;=v")); spark_disconnect(con); "ok"})
t({con <- spark_connect(paste0(URL, "/;X-Foo=v")); spark_disconnect(con); "ok"})
t({con <- spark_connect(URL, "x y" = "v"); spark_disconnect(con); "ok"})
cat("## channel numbers\n")
for (p in c("grpc_max_message_size=1.5","grpc_max_message_size=1e300","grpc_max_message_size=3000000000","grpc_max_message_size=10","grpc_keepalive_time_ms=0.0001","grpc_keepalive_time_ms=1e300","grpc_keepalive_timeout_ms=0.5","grpc_keepalive_enabled=FALSE;grpc_keepalive_time_ms=1e300", "grpc_keepalive_without_calls=zzz")) {
  cat(p, ": ")
  t({con <- spark_connect(paste0(URL, "/;", p)); r <- nrow(spark_collect(spark_sql(con, "select * from range(1000)"))); spark_disconnect(con); r})
}
t({con <- spark_connect(URL, grpc_max_message_size = 2^31); spark_disconnect(con); "ok"})
cat("## dots\n")
t(spark_connect(URL, token = "abc="))   # would TLS to 127.0.0.1 -> expect fail; just class
t(spark_connect(URL, "tok"))
t(spark_connect(c(URL, URL)))
t(spark_connect(NA_character_)); t(spark_connect("")); t(spark_connect(NULL)); t(spark_connect(factor(URL)))
