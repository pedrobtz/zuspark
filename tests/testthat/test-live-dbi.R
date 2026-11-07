# DBI and dbplyr against a live server (Stage 11). live.yaml installs DBI,
# dbplyr and dplyr, so these never skip there.

test_that("live: DBI queries, quoting and fetching in chunks", {
  con <- local_live_dbi()
  expect_identical(DBI::dbGetQuery(con, "SELECT id FROM range(3)")$id, c(0, 1, 2))
  tricky <- "it's a \\ back\\slash"
  got <- DBI::dbGetQuery(con, paste("SELECT", DBI::dbQuoteString(con, tricky), "AS s"))$s
  expect_identical(got, tricky)
  odd <- "odd `name`"
  expect_identical(names(DBI::dbGetQuery(con, paste("SELECT 1 AS", DBI::dbQuoteIdentifier(con, odd)))), odd)

  res <- DBI::dbSendQuery(con, "SELECT * FROM range(10)")
  expect_identical(DBI::dbColumnInfo(res)$type, "bigint")
  expect_identical(nrow(DBI::dbFetch(res, 4)), 4L)
  expect_identical(nrow(DBI::dbFetch(res)), 6L)
  expect_true(DBI::dbHasCompleted(res))
  DBI::dbClearResult(res)
})

test_that("live: a data frame round-trips through a temporary table", {
  con <- local_live_dbi()
  df <- data.frame(
    i = 1:3, d = c(1.5, NA, -2), s = c("a", "b'c", NA), b = c(TRUE, FALSE, NA),
    day = as.Date("2026-11-07") + 0:2, stringsAsFactors = FALSE
  )
  DBI::dbWriteTable(con, "zuspark_live_dbi", df, temporary = TRUE, overwrite = TRUE)
  expect_true(DBI::dbExistsTable(con, "zuspark_live_dbi"))
  expect_contains(DBI::dbListTables(con), "zuspark_live_dbi")
  expect_identical(DBI::dbListFields(con, "zuspark_live_dbi"), names(df))
  expect_identical(DBI::dbReadTable(con, "zuspark_live_dbi"), df)
  expect_error(DBI::dbWriteTable(con, "zuspark_live_dbi", df, temporary = TRUE), class = "zuspark_error")
  DBI::dbRemoveTable(con, "zuspark_live_dbi")
  expect_false(DBI::dbExistsTable(con, "zuspark_live_dbi"))
})

test_that("live: dbplyr runs a pipeline on Spark", {
  testthat::skip_if_not_installed("dbplyr")
  testthat::skip_if_not_installed("dplyr")
  con <- local_live_dbi()
  sales <- dplyr::copy_to(con, data.frame(day = rep(1:3, 4), amount = 1:12), "zuspark_live_sales", overwrite = TRUE)
  got <- dplyr::collect(dplyr::arrange(
    dplyr::summarise(
      dplyr::group_by(dplyr::filter(sales, amount > 2), day),
      n = dplyr::n(), total = sum(amount, na.rm = TRUE)
    ),
    day
  ))
  expect_identical(got$day, 1:3)
  expect_identical(got$total, c(21, 24, 30))

  # The work stays on the server: a million rows, seven back.
  counts <- dplyr::collect(dplyr::count(
    dplyr::tbl(con, dbplyr::sql("SELECT id % 7 AS weekday FROM range(1000000)")),
    weekday
  ))
  expect_identical(nrow(counts), 7L)
  expect_identical(sum(counts$n), 1e6)
})

test_that("live: appends go by column name, and permanent tables overwrite", {
  con <- local_live_dbi()
  withr::defer(DBI::dbRemoveTable(con, "zuspark_live_names", fail_if_missing = FALSE))
  DBI::dbRemoveTable(con, "zuspark_live_names", fail_if_missing = FALSE)
  DBI::dbWriteTable(con, "zuspark_live_names", data.frame(first = "Ada", last = "Lovelace"))
  DBI::dbAppendTable(con, "zuspark_live_names", data.frame(last = "Hopper", first = "Grace"))
  got <- DBI::dbReadTable(con, "zuspark_live_names")
  expect_identical(got$last[got$first == "Grace"], "Hopper")
  expect_true(is.na(DBI::dbExecute(con, "INSERT INTO zuspark_live_names VALUES ('Alan', 'Turing')")))

  DBI::dbWriteTable(con, "zuspark_live_names", data.frame(first = "Only", last = "One"), overwrite = TRUE)
  expect_identical(nrow(DBI::dbReadTable(con, "zuspark_live_names")), 1L)
})

test_that("live: a timestamp keeps its instant in a session that is not UTC", {
  con <- local_live_dbi()
  old <- spark_config(con, spark.sql.session.timeZone = "America/Los_Angeles")
  withr::defer(spark_config(con, spark.sql.session.timeZone = old[[1]]))
  t0 <- as.POSIXct("2026-11-07 12:00:00", tz = "UTC")
  DBI::dbWriteTable(con, "zuspark_live_ts", data.frame(t = t0), temporary = TRUE, overwrite = TRUE)
  back <- DBI::dbReadTable(con, "zuspark_live_ts")$t
  expect_equal(as.numeric(back), as.numeric(t0))
})

test_that("live: doubles at the edges, and DECIMAL columns under tbl()", {
  testthat::skip_if_not_installed("dbplyr")
  con <- local_live_dbi()
  d <- c(Inf, -Inf, NaN, NA, 1e-300, 1e300)
  DBI::dbWriteTable(con, "zuspark_live_dbl", data.frame(d = d), temporary = TRUE, overwrite = TRUE)
  back <- DBI::dbReadTable(con, "zuspark_live_dbl")$d
  expect_identical(is.nan(back), is.nan(d))
  expect_identical(back[!is.na(back)], d[!is.na(d)])
  # dbplyr reads the columns with no rows; a DECIMAL must not stop it.
  expect_identical(colnames(dplyr::tbl(con, dbplyr::sql("SELECT CAST(1.5 AS DECIMAL(5,2)) AS d"))), "d")
})

test_that("live: copy_to() overwrites a permanent table", {
  testthat::skip_if_not_installed("dbplyr")
  con <- local_live_dbi()
  withr::defer(DBI::dbRemoveTable(con, "zuspark_live_copy", fail_if_missing = FALSE))
  dplyr::copy_to(con, data.frame(x = 1:3), "zuspark_live_copy", temporary = FALSE, overwrite = TRUE)
  p <- dplyr::copy_to(con, data.frame(x = 4:5), "zuspark_live_copy", temporary = FALSE, overwrite = TRUE)
  expect_identical(sort(dplyr::pull(p, x)), 4:5)
})
