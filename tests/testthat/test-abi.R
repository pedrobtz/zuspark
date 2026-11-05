# zuspark is pure R (design D3): no src/, so no shared library is built or
# loaded. Native code belongs to zuhttp and zuarrow.

test_that("the installed package has no compiled code", {
  expect_identical(system.file("libs", package = "zuspark"), "")
  expect_false("zuspark" %in% names(getLoadedDLLs()))
})
