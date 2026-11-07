# The Spark Connect descriptor set (design §5, D4). tools/build-descriptors.sh
# builds it at a pinned Spark tag; it is loaded into one zuproto pool the
# first time a message is encoded or decoded, and kept for the session.

the <- new.env(parent = emptyenv())

spark_pool <- function() {
  if (is.null(the$pool)) {
    the$pool <- zuproto::proto_pool(spark_descriptor_path())
  }
  the$pool
}

spark_descriptor_path <- function() {
  system.file(
    "proto",
    "spark-connect.binpb",
    package = "zuspark",
    mustWork = TRUE
  )
}

# inst/proto/VERSION as a named character vector: spark, googleapis, protoc
# and sha256.
descriptor_version <- function() {
  path <- system.file("proto", "VERSION", package = "zuspark", mustWork = TRUE)
  lines <- readLines(path, warn = FALSE)
  keys <- sub(":.*$", "", lines)
  values <- trimws(sub("^[^:]*:", "", lines))
  stats::setNames(values, keys)
}
