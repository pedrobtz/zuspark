# Every condition zuspark raises inherits from zuspark_error (design §8).
# Fields given in `...` become fields of the condition; `parent` keeps the
# condition from a lower layer, such as zugrpc's.

zuspark_abort <- function(class, message, ..., parent = NULL, call = NULL) {
  cnd <- errorCondition(
    message,
    ...,
    parent = parent,
    class = c(class, "zuspark_error"),
    call = call
  )
  stop(cnd)
}
