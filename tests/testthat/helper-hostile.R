# Helpers and data for test-hostile.R, kept out of the test file so that
# devtools::test(shuffle = TRUE) cannot run a test before them.

# Runs connect, spark_sql() and spark_collect() with `responses` applied,
# and returns the data frame or the condition.
hostile_run <- function(responses) {
  local_replay("sql-query", responses = responses)
  tryCatch(
    {
      con <- replay_connect("sql-query")
      on.exit(close_connection(con), add = TRUE)
      q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
      spark_collect(q)
    },
    error = identity
  )
}

# Damages message `m` of streaming call `n` with `f`.
on_message <- function(n, m, f) {
  stats::setNames(list(function(messages) {
    messages[[m]] <- f(messages[[m]])
    messages
  }), n)
}

on_unary <- function(n, f) {
  stats::setNames(list(function(bytes) f(bytes)), n)
}

expect_protocol_error <- function(x, label) {
  if (!inherits(x, "zuspark_protocol_error")) {
    fail(sprintf("%s: expected zuspark_protocol_error, got %s", label, paste(class(x), collapse = "/")))
  } else {
    succeed()
  }
}

expect_zuspark_outcome <- function(x, label) {
  # A valid value R cannot hold exactly, refused as the caller asked.
  lossy <- inherits(x, "zuarrow_lossy_error")
  # A valid stream that stops before ResultComplete: zuspark reattaches,
  # which the recorded scenario has no answer for.
  reattach <- inherits(x, "simpleError") && grepl("not ReattachExecute", conditionMessage(x), fixed = TRUE)
  if (inherits(x, "condition") && !inherits(x, "zuspark_error") && !lossy && !reattach) {
    fail(sprintf("%s: %s: %s", label, paste(class(x), collapse = "/"), conditionMessage(x)))
  } else {
    succeed()
  }
}

flip <- function(bytes, at) {
  bytes[at] <- as.raw(bitwXor(as.integer(bytes[at]), 0xFFL))
  bytes
}

stream_messages <- function(n) {
  read_stream_fixture("sql-query", fixture_calls("sql-query")$stem[[n]])
}

# Message m of call n decoded, edited, encoded again.
edit_message <- function(n, m, f) {
  on_message(n, m, function(bytes) {
    x <- zuproto::proto_decode(bytes, "spark.connect.ExecutePlanResponse", spark_pool())
    zuproto::proto_encode(f(x), "spark.connect.ExecutePlanResponse", spark_pool())
  })
}

batch_message <- function() {
  msgs <- stream_messages(5)
  which(vapply(msgs, function(b) {
    !is.null(zuproto::proto_decode(b, "spark.connect.ExecutePlanResponse", spark_pool())$arrow_batch)
  }, logical(1)))[[1L]]
}
