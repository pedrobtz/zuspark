# The shipped descriptor set (design §5, D4).

test_that("the descriptor set loads into a zuproto pool", {
  types <- zuproto::proto_types(spark_pool())$name
  expect_contains(types, c(
    "spark.connect.ExecutePlanRequest",
    "spark.connect.ExecutePlanResponse",
    "spark.connect.SqlCommand",
    "google.rpc.Status",
    "google.rpc.ErrorInfo"
  ))
})

test_that("the pool is built once per session", {
  expect_identical(spark_pool(), spark_pool())
})

test_that("VERSION records the pins and the set's SHA-256", {
  version <- descriptor_version()
  expect_identical(version[["spark"]], "v4.2.1")
  expect_match(version[["googleapis"]], "^[0-9a-f]{40}$")
  expect_match(version[["sha256"]], "^[0-9a-f]{64}$")

  # tools::sha256sum() arrived in R 4.5.0.
  skip_if_not(exists("sha256sum", envir = asNamespace("tools")))
  sum <- tools::sha256sum(spark_descriptor_path())
  expect_identical(unname(sum), version[["sha256"]])
})
