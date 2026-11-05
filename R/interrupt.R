# Interrupts (design §7.9). An R interrupt while a stream is read cancels
# the call and then asks the server to interrupt the operation, so that the
# job stops rather than only the transfer.

#' Interrupt operations on the server
#'
#' `spark_interrupt()` asks the server to stop operations of this session:
#' every one, or one by its operation id. An R interrupt (Ctrl-C, Esc)
#' while zuspark waits for a result does this for the running query by
#' itself, and raises a `zuspark_interrupt_error`.
#'
#' @inheritParams spark_disconnect
#' @param x `NULL` for every operation of the session, an operation id, or
#'   a `zuspark_query`, for the last execution of that query.
#' @return The ids of the operations the server interrupted, invisibly.
#' @export
#' @examplesIf nzchar(Sys.getenv("SPARK_REMOTE"))
#' con <- spark_connect()
#' spark_interrupt(con)
#' spark_disconnect(con)
spark_interrupt <- function(con, x = NULL) {
  check_open(con)
  if (inherits(x, "zuspark_query")) {
    x <- x$operation_id
    if (is.null(x)) {
      return(invisible(character()))
    }
  }
  if (!is.null(x) && (!is.character(x) || length(x) != 1L || is.na(x))) {
    zuspark_abort("zuspark_error", "`x` must be NULL, an operation id or a query.")
  }
  invisible(interrupt_rpc(con, x))
}

interrupt_rpc <- function(con, operation_id = NULL, deadline = con$deadline) {
  request <- session_fields(con)
  if (is.null(operation_id)) {
    request$interrupt_type <- "INTERRUPT_TYPE_ALL"
  } else {
    request$interrupt_type <- "INTERRUPT_TYPE_OPERATION_ID"
    request$operation_id <- operation_id
  }
  response <- spark_rpc(
    con, "Interrupt", request,
    "spark.connect.InterruptRequest", "spark.connect.InterruptResponse",
    deadline = deadline
  )
  observe_session(con, response)
  as.character(unlist(response$interrupted_ids))
}

# Called when an R interrupt reached a running execution: cancel the
# stream, interrupt the operation on the server, and raise.
interrupt_execution <- function(con, op, stream) {
  try(stream$cancel(), silent = TRUE)
  ids <- tryCatch(interrupt_rpc(con, op), error = function(e) character())
  zuspark_abort(
    "zuspark_interrupt_error",
    sprintf(
      "Interrupted: the server stopped %s.",
      if (length(ids)) sprintf("operation %s", paste(ids, collapse = ", ")) else "no operation (it had finished)"
    ),
    interrupted_ids = ids,
    operation_id = op
  )
}
