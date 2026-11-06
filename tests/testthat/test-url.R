# The connection string (design §7.1). PySpark's four INVALID_CONNECT_URL
# messages, from DefaultChannelBuilder at v4.2.1, are the test cases.

test_that("PySpark's four connection-string errors are reproduced", {
  expect_error(
    parse_spark_url("http://localhost:15002"),
    "The URL must start with 'sc://'. Please update the URL to follow the correct format, e.g., 'sc://hostname:port'.",
    fixed = TRUE,
    class = "zuspark_url_error"
  )
  expect_error(
    parse_spark_url("sc://localhost:15002/path"),
    "The path component '/path' must be empty. Please update the URL to follow the correct format, e.g., 'sc://hostname:port'.",
    fixed = TRUE,
    class = "zuspark_url_error"
  )
  expect_error(
    parse_spark_url("sc://localhost/;token"),
    "Parameter 'token' should be provided as a key-value pair separated by an equal sign (=). Please update the parameter to follow the correct format, e.g., 'key=value'.",
    fixed = TRUE,
    class = "zuspark_url_error"
  )
  expect_error(
    parse_spark_url("sc://:15002"),
    "Hostname is missing in the URL: 'sc://:15002'. Please update the URL to follow the correct format, e.g., 'sc://hostname:port'.",
    fixed = TRUE,
    class = "zuspark_url_error"
  )
})

test_that("a url error names the part that failed", {
  cnd <- tryCatch(parse_spark_url("sc://localhost/x"), error = identity)
  expect_s3_class(cnd, "zuspark_error")
  expect_identical(cnd$part, "path")
  cnd <- tryCatch(parse_spark_url("sc://localhost:http"), error = identity)
  expect_identical(cnd$part, "port")
})

test_that("host and port take PySpark's defaults", {
  u <- parse_spark_url("sc://localhost")
  expect_identical(u$host, "localhost")
  expect_identical(u$port, 15002L)
  expect_identical(u$url, "http://localhost:15002")
  expect_false(u$tls)

  u <- parse_spark_url("sc://spark.example.com:443/")
  expect_identical(u$port, 443L)
  expect_identical(u$url, "http://spark.example.com:443")
})

test_that("an IPv6 host stays bracketed", {
  u <- parse_spark_url("sc://[::1]:15003")
  expect_identical(u$host, "[::1]")
  expect_identical(u$url, "http://[::1]:15003")
})

test_that("parameters are split on ';', percent-decoded and case-sensitive", {
  u <- parse_spark_url("sc://h/;user_id=a%20b;User_Id=c;x-extra=1%3B2")
  expect_identical(u$user_id, "a b")
  expect_identical(u$headers, c(User_Id = "c", `x-extra` = "1;2"))
})

test_that("reserved parameters never become headers; the rest all do", {
  u <- parse_spark_url(paste0(
    "sc://h/;token=t;use_ssl=false;user_id=u;user_agent=a;",
    "session_id=0b3e1c3a-8a0b-4e0b-9a8c-1f2d3e4f5a6b;",
    "grpc_max_message_size=1024;grpc_keepalive_enabled=true;",
    "grpc_keepalive_time_ms=1000;grpc_keepalive_timeout_ms=500;",
    "grpc_keepalive_without_calls=false;x-databricks-cluster-id=c1"
  ))
  expect_identical(u$headers, c(`x-databricks-cluster-id` = "c1"))
  expect_identical(u$max_message, 1024)
  expect_identical(u$keepalive$time, 1)
  expect_identical(u$keepalive$timeout, 0.5)
})

test_that("a token turns TLS on, except on localhost without use_ssl", {
  expect_true(parse_spark_url("sc://h/;token=t")$tls)
  expect_true(parse_spark_url("sc://h/;use_ssl=true")$tls)
  expect_true(parse_spark_url("sc://h/;use_ssl=TRUE")$tls)
  expect_identical(parse_spark_url("sc://h/;token=t")$url, "https://h:15002")
  # PySpark uses local channel credentials here: no TLS, token still sent.
  u <- parse_spark_url("sc://localhost/;token=t")
  expect_false(u$tls)
  expect_identical(u$token, "t")
  expect_true(parse_spark_url("sc://localhost/;token=t;use_ssl=true")$tls)
})

test_that("the token falls back to SPARK_CONNECT_AUTHENTICATE_TOKEN", {
  withr::local_envvar(SPARK_CONNECT_AUTHENTICATE_TOKEN = "from-env")
  expect_identical(parse_spark_url("sc://h")$token, "from-env")
  expect_identical(parse_spark_url("sc://h/;token=t")$token, "t")
})

test_that("session_id must be a UUID, and is sent as written", {
  expect_error(
    parse_spark_url("sc://h/;session_id=not-a-uuid"),
    class = "zuspark_url_error"
  )
  # Python's uuid.UUID(x, version = 4) rewrites the version bits rather than
  # checking them, so PySpark accepts any well-formed UUID; so does zuspark.
  id <- "0B3E1C3A-8A0B-1E0B-9A8C-1F2D3E4F5A6B"
  expect_identical(parse_spark_url(paste0("sc://h/;session_id=", id))$session_id, id)
})

test_that("a missing host is reported against the string as given", {
  # PySpark's message quotes its http:// rewrite; zuspark quotes the input.
  expect_error(parse_spark_url("sc://"), "'sc://'", class = "zuspark_url_error")
})

test_that("a parameter with two equal signs is not a key-value pair", {
  # Only the key is quoted: the value may be a credential.
  expect_error(parse_spark_url("sc://h/;a=b=c"), "Parameter 'a' ", class = "zuspark_url_error")
})

test_that("parse errors never print the token", {
  # A base64 token with padding has '=' in it.
  err <- tryCatch(parse_spark_url("sc://host/;token=c2VjcmV0=="), error = identity)
  expect_s3_class(err, "zuspark_url_error")
  expect_false(grepl("c2VjcmV0", conditionMessage(err), fixed = TRUE))
  expect_match(conditionMessage(err), "Parameter 'token' ", fixed = TRUE)

  err <- tryCatch(parse_spark_url("sc://:15002/;token=SECRET123"), error = identity)
  expect_s3_class(err, "zuspark_url_error")
  expect_false(grepl("SECRET123", conditionMessage(err), fixed = TRUE))
  expect_match(conditionMessage(err), "'sc://:15002/;token=<redacted>'", fixed = TRUE)
})

test_that("a numeric parameter must be a number", {
  expect_error(
    parse_spark_url("sc://h/;grpc_max_message_size=big"),
    class = "zuspark_url_error"
  )
})
