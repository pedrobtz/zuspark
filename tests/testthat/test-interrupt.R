# Interrupts (design §7.9).

test_that("an R interrupt during a stream cancels it and interrupts the operation", {
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  release <- read_fixture("sql-query", fixture_calls("sql-query")$stem[[6]], "response")
  first <- read_stream_fixture("sql-query", fixture_calls("sql-query")$stem[[5]])[1]
  sent <- list()
  cancelled <- FALSE
  con$transport <- function(con, method, bytes, deadline, streaming = FALSE) {
    sent[[length(sent) + 1L]] <<- list(method = method, request = zuproto::proto_decode(bytes, request_types[[method]], spark_pool(), enums = "character"))
    switch(method,
      ExecutePlan = {
        s <- replay_stream(c(first, list(structure(class = c("interrupt", "condition"), list(message = "", call = NULL)))))
        s$cancel <- function() cancelled <<- TRUE
        s
      },
      Interrupt = interrupt_response(con, sent[[1]]$request$operation_id),
      ReleaseExecute = release
    )
  }
  err <- tryCatch(spark_collect(q), error = identity)
  expect_s3_class(err, c("zuspark_interrupt_error", "zuspark_error"))
  op <- sent[[1]]$request$operation_id
  expect_identical(err$interrupted_ids, op)
  expect_identical(q$operation_id, op)
  expect_true(cancelled)
  methods <- vapply(sent, `[[`, "", "method")
  expect_identical(methods, c("ExecutePlan", "Interrupt", "ReleaseExecute"))
  expect_identical(sent[[2]]$request$interrupt_type, "INTERRUPT_TYPE_OPERATION_ID")
  expect_identical(sent[[2]]$request$operation_id, op)
  close_connection(con)
})

test_that("zugrpc's own interrupt condition takes the same path", {
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  release <- read_fixture("sql-query", fixture_calls("sql-query")$stem[[6]], "response")
  con$transport <- function(con, method, bytes, deadline, streaming = FALSE) {
    switch(method,
      ExecutePlan = replay_stream(list(structure(
        class = c("zugrpc_error_interrupt", "zugrpc_error", "error", "condition"),
        list(message = "interrupted", call = NULL)
      ))),
      Interrupt = interrupt_response(con, character()),
      ReleaseExecute = release
    )
  }
  err <- tryCatch(spark_collect(q), error = identity)
  expect_s3_class(err, "zuspark_interrupt_error")
  expect_identical(err$interrupted_ids, character())
  expect_match(conditionMessage(err), "no operation")
  close_connection(con)
})

test_that("spark_interrupt() interrupts everything, one id, or a query's last run", {
  local_replay("connect")
  con <- replay_connect("connect")
  sent <- list()
  con$transport <- function(con, method, bytes, deadline, streaming = FALSE) {
    req <- zuproto::proto_decode(bytes, "spark.connect.InterruptRequest", spark_pool(), enums = "character")
    sent[[length(sent) + 1L]] <<- req
    interrupt_response(con, if (nzchar(req$operation_id %||% "")) req$operation_id else c("a", "b"))
  }
  expect_identical(spark_interrupt(con), c("a", "b"))
  expect_identical(sent[[1]]$interrupt_type, "INTERRUPT_TYPE_ALL")
  expect_identical(spark_interrupt(con, "op-1"), "op-1")
  expect_identical(sent[[2]]$interrupt_type, "INTERRUPT_TYPE_OPERATION_ID")
  q <- new_query(con, "SELECT 1", list())
  expect_identical(spark_interrupt(con, q), character())
  q$operation_id <- "op-2"
  expect_identical(spark_interrupt(con, q), "op-2")
  expect_error(spark_interrupt(con, 1), class = "zuspark_error")
  close_connection(con)
})

test_that("an interrupt during a unary call is a zuspark_interrupt_error", {
  local_replay("schema")
  con <- replay_connect("schema")
  q <- spark_sql(con, schema_sql)
  sent <- character()
  con$transport <- function(con, method, bytes, deadline, streaming = FALSE) {
    sent <<- c(sent, method)
    stop(structure(
      class = c("zugrpc_error_interrupt", "zugrpc_error", "error", "condition"),
      list(message = "interrupted", call = NULL)
    ))
  }
  err <- tryCatch(spark_schema(q), error = identity)
  expect_s3_class(err, "zuspark_interrupt_error")
  expect_s3_class(err, "zugrpc_error_interrupt")
  # A unary call is only cancelled: no Interrupt is sent.
  expect_identical(sent, "AnalyzePlan")
  close_connection(con)
})

test_that("a failed Interrupt call is told apart from a finished operation", {
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  release <- read_fixture("sql-query", fixture_calls("sql-query")$stem[[6]], "response")
  con$transport <- function(con, method, bytes, deadline, streaming = FALSE) {
    switch(method,
      ExecutePlan = replay_stream(list(structure(
        class = c("zugrpc_error_interrupt", "zugrpc_error", "error", "condition"),
        list(message = "interrupted", call = NULL)
      ))),
      Interrupt = stop(grpc_transport_condition("connection reset")),
      ReleaseExecute = release
    )
  }
  err <- tryCatch(spark_collect(q), error = identity)
  expect_s3_class(err, "zuspark_interrupt_error")
  expect_match(conditionMessage(err), "could not be asked", fixed = TRUE)
  expect_s3_class(err$interrupt_failure, "zuspark_rpc_error")
  close_connection(con)
})

test_that("spark_interrupt() refuses a query of another connection", {
  local_replay("sql-query")
  con <- replay_connect("sql-query")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  # A second connection: a copy of the first, which is not identical() to it.
  other <- new.env(parent = emptyenv())
  for (n in ls(con, all.names = TRUE)) assign(n, get(n, con), other)
  class(other) <- class(con)
  expect_error(spark_interrupt(other, q), class = "zuspark_error")
  close_connection(con)
})
