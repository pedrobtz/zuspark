source("common.R")
mode <- commandArgs(TRUE)[[1]]
P <- zuspark:::spark_pool()
if (mode == "dbx") {
  reqs <- list()
  fake <- function(con, method, bytes, deadline, streaming = FALSE) {
    req <- zuproto::proto_decode(bytes, sprintf("spark.connect.%sRequest", method), P, enums = "character")
    reqs[[length(reqs) + 1L]] <<- req
    sid <- req$session_id
    switch(method,
      Config = zuproto::proto_encode(list(session_id = sid, server_side_session_id = "srv"), "spark.connect.ConfigResponse", P),
      AnalyzePlan = zuproto::proto_encode(list(session_id = sid, server_side_session_id = "srv", spark_version = list(version = "4.2.1")), "spark.connect.AnalyzePlanResponse", P),
      ReleaseSession = zuproto::proto_encode(list(session_id = sid, server_side_session_id = "srv"), "spark.connect.ReleaseSessionResponse", P))
  }
  options(zuspark.transport = fake)
  chan <- function(con) { ch <- zuspark:::channels[[con$channel_key]]; ch }
  con <- t(spark_connect_databricks("https://DBC-1.cloud.databricks.com/?o=1", token = "dapi+abc/=%;x y"), FALSE)
  print(con); ch <- chan(con); str(get("headers", ch)); str(get("origin", ch))
  hd <- function(con) { h <- get("headers", chan(con)); if (is.list(h)) h <- unlist(h); h }
  cat("session header == session id:", identical(unname(hd(con)[["x-databricks-session-id"]]), con$session_id), "\n")
  U <- toupper(zuspark:::new_uuid())
  for (a in list(list(session_id = U), list(cluster_id = "0123-abc"), list(cluster_id = "0123-abc", session_id = U), list(cluster_id = ""), list(cluster_id = NA), list(cluster_id = c("a", "b")), list(cluster_id = 5), list(cluster_id = "zoë"),
                 list(headers = c("x-databricks-session-id" = "mine")), list(headers = c("X-Databricks-Session-Id" = "mine")), list(headers = list(a = "1")), list(user_id = "u1", user_agent = "ag", deadline = 5), list(use_ssl = FALSE), list(token2 = "x"), list(session_id = zuid::uuid(version = 4)), list(headers = c(authorization = "Basic zzz")))) {
    cat("args:", deparse(a, width.cutoff = 200L), "\n")
    con <- t(do.call(spark_connect_databricks, c(list("dbc-1.cloud.databricks.com", "tok"), a)), FALSE)
    if (!inherits(con, "err")) { h <- hd(con); h[["authorization"]] <- sub("Bearer .*", "Bearer <tok>", h[["authorization"]]); cat("   endpoint:", con$endpoint, " url:", con$url, "\n   headers:", paste(names(h), h, sep = "=", collapse = " | "), "\n   session:", con$session_id, " user:", con$user_id, " deadline:", con$deadline, "\n") }
  }
  for (tk in list("", NA, NULL, c("a", "b"), 5, " tok ", "tok\n")) { cat("token:", deparse(tk), ": "); t(class(spark_connect_databricks("x.com", tk))) }
  Sys.setenv(DATABRICKS_HOST = "https://envhost.com", DATABRICKS_TOKEN = "envtok"); con <- t(spark_connect_databricks(), FALSE); print(con)
  cat("## plain connect: token routes\n")
  for (tk in c("abc=", "abc==", "a+b/c", "a%b", "a;b", "a b", "dapi-123")) {
    c1 <- t(spark_connect("sc://h.example", token = tk), FALSE); c2 <- t(spark_connect(paste0("sc://h.example/;token=", tk)), FALSE)
    cat(sprintf("token %-9s dots: %-18s url: %s\n", tk, if (inherits(c1, "err")) "ERR" else hd(c1)[["authorization"]], if (inherits(c2, "err")) paste("ERR", c2[[2]]) else hd(c2)[["authorization"]]))
  }
  Sys.setenv(SPARK_CONNECT_AUTHENTICATE_TOKEN = "envtoken"); c3 <- t(spark_connect("sc://h.example"), FALSE); cat("env token:", hd(c3)[["authorization"]], c3$endpoint, "\n"); c4 <- t(spark_connect("sc://h.example", token = "arg"), FALSE); cat("arg wins:", hd(c4)[["authorization"]], "\n"); Sys.unsetenv("SPARK_CONNECT_AUTHENTICATE_TOKEN")
  cat("## printed url after ... \n")
  print(t(spark_connect("sc://u:pw@h.example", token = "sekret", apikey = "k", use_ssl = TRUE, user_id = "me"), FALSE))
  print(t(spark_connect("sc://u:p;w@h.example/"), FALSE))
  c5 <- t(spark_connect("sc://h.example?x=1", token = "t"), FALSE); cat("token after '?': endpoint", c5$endpoint, " auth header:", deparse(hd(c5)[["authorization"]]), "\n")
  cat("## version / session checks\n")
  mk <- function(ver, sid_fun = identity) function(con, method, bytes, deadline, streaming = FALSE) { req <- zuproto::proto_decode(bytes, sprintf("spark.connect.%sRequest", method), P); zuproto::proto_encode(c(list(session_id = sid_fun(req$session_id), server_side_session_id = "srv"), if (method == "AnalyzePlan") list(spark_version = list(version = ver))), sprintf("spark.connect.%sResponse", method), P) }
  for (v in c("3.5.0", "3.4.9", "3.10.0", "4.0.0-preview1", "v4.0.0", "", "4", "3.5", "10.0.0", "3.5.0.1", "4.1.0-SNAPSHOT", "abc", "3.5.0-amzn-0", " 4.0.0")) { options(zuspark.transport = mk(v)); r <- t(spark_connect("sc://h"), FALSE); cat(sprintf("version '%s' -> %s\n", v, if (inherits(r, "err")) r[[2]] else paste("ok, spark_version() =", deparse(spark_version(r))))) }
  options(zuspark.transport = mk("4.0.0", toupper)); t(spark_connect("sc://h"))
  options(zuspark.transport = mk("4.0.0", tolower)); t(class(spark_connect("sc://h", session_id = toupper(zuspark:::new_uuid()))))
}
if (mode == "C") {
  cat(Sys.getlocale("LC_CTYPE"), "\n")
  l1 <- "caf\xe9"; Encoding(l1) <- "latin1"; u8 <- "café"
  show <- function(x) cat(Encoding(x), paste(charToRaw(x), collapse = ""), "\n")
  show(zuspark:::spark_quote_string(l1)); show(zuspark:::spark_quote_string(u8)); show(zuspark:::spark_quote_identifier(l1)); show(zuspark:::spark_quote_identifier(u8))
  show(zuspark:::spark_literals(l1)); show(zuspark:::spark_literals(factor(l1))); show(zuspark:::spark_values_select(data.frame(x = l1)))
  show(zuspark:::scalar_text(l1)); show(zuspark:::append_url_params("sc://h", list(user_id = l1)))
  con <- spark_connect(URL)
  r <- t(spark_collect(spark_sql(con, paste0("SELECT '", l1, "' AS x")))$x, FALSE); if (!inherits(r, "err")) show(r)
  r <- t({spark_config(con, zz.l = l1); spark_config(con, "zz.l")}, FALSE); if (!inherits(r, "err")) show(r)
  spark_disconnect(con)
}
if (mode == "json") {
  con <- spark_connect(URL)
  for (s in c("SELECT INTERVAL '1' DAY / 0", "SELECT CAST('x' AS INT)", "SELECT assert_true(false)", "SELECT 1 +", "CREATE TABLE zz (a INT) USING nope", "SELECT element_at(array(1), 5)", "SELECT * FROM range(1) AS t(a, b)")) {
    e <- tryCatch({x <- spark_sql(con, s); if (inherits(x, "zuspark_query")) spark_collect(x)}, error = identity)
    cat(s, "\n   class:", class(e)[1], " error_class:", e$error_class, " sql_state:", e$sql_state, "\n   raw messageParameters:", e$parent$details$details$ErrorInfo$metadata[["messageParameters"]], "\n   parsed:", deparse(e$message_parameters), "\n")
  }
  spark_disconnect(con)
}
