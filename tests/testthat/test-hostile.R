# Hostile responses (design §9): truncations, byte flips and oversized
# counts in recorded responses give zuspark_protocol_error, never a crash.
# A cut or a byte flip can leave a message well-formed (a different id, a
# different value, a stream without its end), so for those the rule is
# weaker: the call succeeds, fails with a zuspark_error, refuses a value as
# zuarrow's lossy rule says, or (a stream left without ResultComplete)
# reattaches, which the replay answers with its own error; never anything
# else.
#
# Every case replays `sql-query` with one response damaged: call 1 is
# Config (connect), 2 AnalyzePlan (version), 3 the SqlCommand's
# ExecutePlan, 5 the query's ExecutePlan.

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

test_that("the undamaged scenario collects", {
  expect_identical(nrow(hostile_run(list())), 5L)
})

test_that("a truncated response message is a protocol error", {
  # Cuts inside the message's last field make it unreadable; a cut on a
  # field boundary is a shorter, valid message, so only the cuts inside
  # the first field and the last byte are asserted exactly.
  for (n in c(3L, 5L)) {
    msgs <- stream_messages(n)
    for (m in seq_along(msgs)) {
      len <- length(msgs[[m]])
      for (cut in unique(round(seq(1L, len - 1L, length.out = 8L)))) {
        x <- hostile_run(on_message(n, m, function(b) b[seq_len(cut)]))
        expect_zuspark_outcome(x, sprintf("call %d message %d cut at %d", n, m, cut))
      }
      x <- hostile_run(on_message(n, m, function(b) b[seq_len(len - 1L)]))
      expect_protocol_error(x, sprintf("call %d message %d without its last byte", n, m))
    }
  }
})

test_that("a truncated unary answer is a protocol error", {
  for (n in 1:2) {
    x <- hostile_run(on_unary(n, function(b) b[seq_len(length(b) - 1L)]))
    expect_protocol_error(x, sprintf("call %d without its last byte", n))
    for (cut in c(1L, 2L, 5L)) {
      expect_zuspark_outcome(hostile_run(on_unary(n, function(b) b[seq_len(cut)])), sprintf("call %d cut at %d", n, cut))
    }
  }
})

test_that("byte flips anywhere give a result or a zuspark_error", {
  set.seed(20261106)
  for (n in c(3L, 5L)) {
    msgs <- stream_messages(n)
    for (m in seq_along(msgs)) {
      for (at in unique(sort(sample(length(msgs[[m]]), min(8L, length(msgs[[m]])))))) {
        x <- hostile_run(on_message(n, m, function(b) flip(b, at)))
        expect_zuspark_outcome(x, sprintf("call %d message %d flipped at %d", n, m, at))
      }
    }
  }
  for (n in 1:2) {
    len <- length(read_fixture("sql-query", fixture_calls("sql-query")$stem[[n]], "response"))
    for (at in unique(sort(sample(len, min(15L, len))))) {
      expect_zuspark_outcome(hostile_run(on_unary(n, function(b) flip(b, at))), sprintf("call %d flipped at %d", n, at))
    }
  }
})

test_that("damaged Arrow bytes inside a batch are a protocol error or a zuspark_error", {
  m <- batch_message()
  data <- function(f) edit_message(5L, m, function(x) {
    x$arrow_batch$data <- f(x$arrow_batch$data)
    x
  })
  len <- length(zuproto::proto_decode(stream_messages(5)[[m]], "spark.connect.ExecutePlanResponse", spark_pool())$arrow_batch$data)

  expect_protocol_error(hostile_run(data(function(d) d[seq_len(len %/% 2L)])), "Arrow cut in half")
  expect_protocol_error(hostile_run(data(function(d) d[seq_len(7L)])), "Arrow cut inside the first message's prefix")
  expect_protocol_error(hostile_run(data(function(d) raw(0))), "empty Arrow data")
  expect_protocol_error(hostile_run(data(function(d) as.raw(rep(0xAB, 64)))), "garbage Arrow data")

  set.seed(1106)
  for (cut in unique(sort(sample(len - 1L, 20L)))) {
    expect_zuspark_outcome(hostile_run(data(function(d) d[seq_len(cut)])), sprintf("Arrow cut at %d", cut))
  }
  for (at in unique(sort(sample(len, 30L)))) {
    expect_zuspark_outcome(hostile_run(data(function(d) flip(d, at))), sprintf("Arrow flipped at %d", at))
  }
})

test_that("oversized counts and lengths are protocol errors", {
  m <- batch_message()
  # A row count far beyond the rows the batch holds.
  expect_protocol_error(
    hostile_run(edit_message(5L, m, function(x) {
      x$arrow_batch$row_count <- 2^52
      x
    })),
    "row_count of 2^52"
  )
  # A start offset that is not the running total.
  expect_protocol_error(
    hostile_run(edit_message(5L, m, function(x) {
      x$arrow_batch$start_offset <- 2^40
      x
    })),
    "start_offset of 2^40"
  )
  # A protobuf length prefix far past the end: field 1, length 2^31.
  huge <- as.raw(c(0x0A, 0x80, 0x80, 0x80, 0x80, 0x08, 0x41))
  expect_protocol_error(hostile_run(on_message(5L, m, function(b) huge)), "length-delimited field of 2^31 bytes")
  expect_protocol_error(hostile_run(on_unary(1L, function(b) huge)), "Config answer with a 2^31-byte field")
  # An Arrow IPC message whose metadata length is 2^31 - 1.
  expect_protocol_error(
    hostile_run(edit_message(5L, m, function(x) {
      d <- x$arrow_batch$data
      d[5:8] <- as.raw(c(0xFF, 0xFF, 0xFF, 0x7F))
      x$arrow_batch$data <- d
      x
    })),
    "Arrow metadata length of 2^31 - 1"
  )
})
