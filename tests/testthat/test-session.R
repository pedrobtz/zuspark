# Sessions (design §7.2), replayed from tests/testthat/fixtures/: `session`
# (connect, read, set and unset settings, disconnect) and `connect`
# (connect and disconnect only).

test_that("connect, read, set, unset and disconnect replay", {
  calls <- local_replay("session")
  con <- replay_connect("session")
  expect_s3_class(con, "zuspark_connection")
  expect_identical(calls$methods, c("Config", "AnalyzePlan"))
  expect_identical(spark_version(con), "4.2.1")

  ids <- spark_session_id(con)
  expect_identical(ids[["client"]], fixture_session_id("session"))
  expect_identical(ids[["server"]], decode_fixture("session", 1)$server_side_session_id)

  got <- spark_config(con, "spark.sql.session.timeZone", "zuspark.no.such.key")
  expect_identical(got, c(spark.sql.session.timeZone = "Etc/UTC", zuspark.no.such.key = NA))

  old <- spark_config(con, spark.sql.session.timeZone = "UTC", zuspark.test.key = NULL)
  expect_identical(old, c(spark.sql.session.timeZone = "Etc/UTC", zuspark.test.key = NA))

  expect_true(spark_disconnect(con))
  expect_identical(
    calls$methods,
    c("Config", "AnalyzePlan", "Config", "Config", "Config", "Config", "ReleaseSession")
  )
  expect_false(spark_disconnect(con))
})

test_that("reading every setting returns a named character vector", {
  local_replay("connect")
  # get_all is the first call of every connect; read its answer the same way.
  con <- replay_connect("connect")
  all <- config_pairs(decode_fixture("connect", 1))
  expect_type(all, "character")
  expect_true(all(nzchar(names(all))))
  expect_contains(names(all), "spark.app.name")
  spark_disconnect(con)
})

test_that("every request carries the session, user and client type", {
  calls <- local_replay("connect")
  con <- replay_connect("connect")
  spark_disconnect(con)
  for (req in calls$requests) {
    expect_identical(req$session_id, fixture_session_id("connect"))
    expect_identical(req$user_context$user_id, "zuspark")
    expect_match(req$client_type, "^zuspark zuspark/\\S+ spark/4\\.2\\.1 os/\\S+ r/\\S+$")
  }
})

test_that("the server-side id is sent back once it is known", {
  calls <- local_replay("connect")
  con <- replay_connect("connect")
  spark_disconnect(con)
  server <- decode_fixture("connect", 1)$server_side_session_id
  expect_null(calls$requests[[1]]$client_observed_server_side_session_id)
  expect_identical(calls$requests[[2]]$client_observed_server_side_session_id, server)
})

test_that("the requests have the recorded shape", {
  calls <- local_replay("session")
  con <- replay_connect("session")
  spark_config(con, "spark.sql.session.timeZone", "zuspark.no.such.key")
  spark_config(con, spark.sql.session.timeZone = "UTC", zuspark.test.key = NULL)
  spark_disconnect(con)
  for (n in seq_along(calls$requests)) {
    sent <- calls$requests[[n]]
    recorded <- decode_fixture("session", n, "request")
    sent$client_type <- recorded$client_type <- NULL
    expect_identical(sent, recorded, label = sprintf("request %d", n))
  }
})

test_that("a server below 3.5.0 is refused", {
  old <- decode_fixture("connect", 2)
  old$spark_version$version <- "3.4.1"
  local_replay("connect", responses = list(`2` = encode_response(old, "AnalyzePlan")))
  expect_error(replay_connect("connect"), "Spark 3.4.1", class = "zuspark_version_error")
})

test_that("a changed server-side session id is a session error", {
  local_replay("connect", responses = list(`2` = function(bytes) {
    x <- zuproto::proto_decode(bytes, "spark.connect.AnalyzePlanResponse", spark_pool())
    x$server_side_session_id <- "00000000-0000-4000-8000-000000000000"
    encode_response(x, "AnalyzePlan")
  }))
  expect_error(
    replay_connect("connect"),
    "lost this session",
    class = "zuspark_session_error"
  )
})

test_that("an answer for another client session is a session error", {
  local_replay("connect", responses = list(`1` = function(bytes) {
    x <- zuproto::proto_decode(bytes, "spark.connect.ConfigResponse", spark_pool())
    x$session_id <- "00000000-0000-4000-8000-000000000000"
    encode_response(x, "Config")
  }))
  expect_error(replay_connect("connect"), class = "zuspark_session_error")
})

test_that("a released session cannot be used", {
  local_replay("connect")
  con <- replay_connect("connect")
  close_connection(con)
  expect_error(spark_config(con), "released", class = "zuspark_session_error")
})

test_that("a server without ReleaseSession (Spark 3.5) still disconnects", {
  local_replay("connect", responses = list(`3` = function(bytes) {
    stop(grpc_status_condition("UNIMPLEMENTED", "Method not found"))
  }))
  con <- replay_connect("connect")
  expect_true(spark_disconnect(con))
  expect_true(con$released)
})

test_that("a failed connect closes what it opened", {
  closed <- FALSE
  local_mocked_bindings(close_connection = function(con) {
    closed <<- TRUE
    con$released <- TRUE
  })
  local_replay("connect", responses = list(`2` = function(bytes) {
    stop(grpc_status_condition("UNAVAILABLE", "connection refused"))
  }))
  expect_error(replay_connect("connect"), class = "zugrpc_error_unavailable")
  expect_true(closed)
})

test_that("the connection prints without its token", {
  local_replay("connect")
  con <- replay_connect("connect", url = "sc://fixture.invalid/;token=secret")
  out <- capture.output(print(con))
  expect_false(any(grepl("secret", out)))
  expect_match(out[[1]], "token=<redacted>")
  close_connection(con)
})

test_that("a token is sent as a bearer authorization header", {
  u <- parse_spark_url("sc://fixture.invalid/;token=secret;x-team=a")
  expect_identical(
    connection_metadata(u, c(`x-extra` = "b")),
    c(`x-team` = "a", `x-extra` = "b", authorization = "Bearer secret")
  )
  expect_identical(connection_metadata(parse_spark_url("sc://h")), character())

  local_replay("connect")
  con <- replay_connect("connect", url = "sc://fixture.invalid/;token=secret")
  expect_identical(con$endpoint, "https://fixture.invalid:15002")
  close_connection(con)
})

test_that("`...` and arguments become parameters of the string", {
  expect_identical(append_url_params("sc://h", list(a = "x y")), "sc://h/;a=x%20y")
  expect_identical(append_url_params("sc://h/;b=1", list(c = TRUE)), "sc://h/;b=1;c=true")
  expect_error(append_url_params("sc://h", list("x")), class = "zuspark_url_error")
})

test_that("the client type follows PySpark's layout and limit", {
  expect_match(client_type("my-app"), "^my-app zuspark/")
  withr::local_envvar(SPARK_CONNECT_USER_AGENT = "from-env")
  expect_match(client_type(), "^from-env zuspark/")
  expect_error(client_type(strrep("a", 2049)), class = "zuspark_url_error")
})

test_that("the user id defaults to SPARK_USER, then USER", {
  withr::local_envvar(SPARK_USER = "spark-user", USER = "user")
  expect_identical(default_user_id(), "spark-user")
  withr::local_envvar(SPARK_USER = "")
  expect_identical(default_user_id(), "user")
})

test_that("no connection string is a url error", {
  withr::local_envvar(SPARK_REMOTE = "")
  expect_error(spark_connect(), "SPARK_REMOTE", class = "zuspark_url_error")
})

test_that("mixing reads and sets in one call is refused", {
  local_replay("connect")
  con <- replay_connect("connect")
  expect_error(spark_config(con, "a", b = "1"), "not both", class = "zuspark_error")
  close_connection(con)
})

test_that("zuspark_info() reports the protocol pin", {
  info <- zuspark_info()
  expect_s3_class(info, "zuspark_info")
  expect_identical(info$spark, "v4.2.1")
  expect_output(print(info), "Spark Connect protocol v4.2.1")
})

test_that("a session id with attributes, such as zuid's uuid, is kept as a plain string", {
  local_replay("connect")
  id <- structure(fixture_session_id("connect"), format = "uuid", class = c("zuid_uuid", "zuid"))
  con <- spark_connect("sc://fixture.invalid", session_id = id, user_id = "zuspark")
  expect_identical(spark_session_id(con)[["client"]], fixture_session_id("connect"))
  expect_true(spark_disconnect(con))
})

test_that("invalid connect arguments are zuspark errors naming the part", {
  local_replay("connect")
  bad <- list(
    list(args = list(user_id = 1), part = "user_id"),
    list(args = list(user_id = NA_character_), part = "user_id"),
    list(args = list(user_id = ""), part = "user_id"),
    list(args = list(user_agent = 1), part = "user_agent"),
    list(args = list(user_agent = NA_character_), part = "user_agent"),
    list(args = list(deadline = "soon"), part = "deadline"),
    list(args = list(deadline = -1), part = "deadline"),
    list(args = list(deadline = c(1, 2)), part = "deadline"),
    list(args = list(headers = c("bad name" = "v")), part = "headers"),
    list(args = list(headers = c(`x-ok` = "a\r\nb")), part = "headers"),
    list(args = list(headers = c(`x-ok` = "café")), part = "headers")
  )
  for (b in bad) {
    err <- tryCatch(do.call(spark_connect, c(list("sc://fixture.invalid"), b$args)), error = identity)
    expect_s3_class(err, "zuspark_url_error")
    expect_identical(err$part, b$part)
  }
})

test_that("a header parameter in the string is checked like `headers`", {
  err <- tryCatch(spark_connect("sc://fixture.invalid/;x-bad=a%0Ab"), error = identity)
  expect_s3_class(err, "zuspark_url_error")
  expect_identical(err$part, "headers")
})

test_that("keepalive and message sizes must be finite and positive", {
  for (u in c("sc://h/;grpc_keepalive_time_ms=0", "sc://h/;grpc_keepalive_timeout_ms=0",
              "sc://h/;grpc_max_message_size=1e400", "sc://h/;grpc_max_message_size=0")) {
    expect_error(parse_spark_url(u), class = "zuspark_url_error")
  }
})

test_that("a saved connection, query or error does not hold the token", {
  secret <- "zuspark-test-secret-7f3a"
  url <- sprintf("sc://fixture.invalid/;token=%s", secret)
  # Serialised without the transport binding: in use it is zuspark's own
  # function, which holds no data, but here it is the fake channel, whose
  # closure reaches this test's `secret`.
  holds_token <- function(x, con) {
    fake <- con$transport
    con$transport <- NULL
    on.exit(con$transport <- fake)
    length(grepRaw(secret, serialize(x, NULL), fixed = TRUE)) > 0L
  }
  # Which fields hold it, to say where on failure.
  where_token <- function(x, con) {
    fake <- con$transport
    con$transport <- NULL
    on.exit(con$transport <- fake)
    envs <- if (is.environment(x)) x else as.environment(unclass(x))
    hits <- Filter(function(n) {
      length(grepRaw(secret, serialize(get(n, envs), NULL), fixed = TRUE)) > 0L
    }, ls(envs, all.names = TRUE))
    paste(hits, collapse = ", ")
  }

  local_replay("sql-query")
  con <- replay_connect("sql-query", url = url)
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_false(holds_token(con, con), info = where_token(con, con))
  expect_false(holds_token(q, con), info = where_token(q, con))
  close_connection(con)

  local_replay("error-details", responses = list(`3` = function(messages) {
    list(spark_status_condition(error_id = decode_fixture("error-details", 5, "request")$error_id))
  }))
  con <- replay_connect("error-details", url = url)
  err <- tryCatch(spark_sql(con, "SELECT * FROM no_such_table"), error = identity)
  expect_s3_class(err, "zuspark_sql_error")
  expect_false(holds_token(err, con), info = where_token(err, con))
  close_connection(con)
})

test_that("a connection unserialised in another session cannot reach the channel", {
  local_replay("connect")
  con <- replay_connect("connect")
  copy <- unserialize(serialize(con, NULL))
  # Another R session has no channel under the copy's key.
  rm(list = copy$channel_key, envir = zuspark:::channels)
  expect_error(connection_channel(copy), class = "zuspark_session_error")
  close_connection(con)
})
