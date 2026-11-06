# Helpers and data for test-interrupt.R, kept out of the test file so that
# devtools::test(shuffle = TRUE) cannot run a test before them.

interrupt_response <- function(con, ids) {
  zuproto::proto_encode(
    list(session_id = con$session_id, server_side_session_id = con$server_side_session_id, interrupted_ids = ids),
    "spark.connect.InterruptResponse", spark_pool()
  )
}
