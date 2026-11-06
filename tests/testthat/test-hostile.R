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
