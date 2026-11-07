# DBI and dbplyr (design D5, Stage 11), offline: quoting, literals, and the
# DBI methods over the recorded scenarios.

test_that("Spark SQL quoting: backticks, and strings with backslash escapes", {
  expect_identical(spark_quote_identifier(c("a", "odd `name`")), c("`a`", "`odd ``name```"))
  expect_identical(
    spark_quote_string(c("it's", "back\\slash", NA)),
    c("'it\\'s'", "'back\\\\slash'", "NULL")
  )
})

test_that("R vectors become Spark types and cast literals", {
  expect_identical(
    vapply(list(TRUE, 1L, 1.5, "a", as.Date("2026-11-07"), Sys.time(), factor("x"), list(as.raw(1))), spark_data_type, ""),
    c("BOOLEAN", "INT", "DOUBLE", "STRING", "DATE", "TIMESTAMP", "STRING", "BINARY")
  )
  expect_identical(spark_literals(c(1L, NA)), c("CAST(1 AS INT)", "CAST(NULL AS INT)"))
  expect_identical(spark_literals(100000), "CAST(100000 AS DOUBLE)")
  expect_identical(spark_literals(c(TRUE, NA)), c("CAST(TRUE AS BOOLEAN)", "CAST(NULL AS BOOLEAN)"))
  expect_identical(spark_literals(as.Date("2026-11-07")), "CAST('2026-11-07' AS DATE)")
  expect_identical(spark_literals(list(as.raw(c(0xca, 0xfe)))), "CAST(X'cafe' AS BINARY)")
  expect_error(spark_data_type(complex(1)), class = "zuspark_error")
})

test_that("a data frame becomes a VALUES select, or a typed empty one", {
  expect_identical(
    spark_values_select(data.frame(x = 1:2, y = c("a", NA))),
    "SELECT * FROM VALUES (CAST(1 AS INT), CAST('a' AS STRING)), (CAST(2 AS INT), CAST(NULL AS STRING)) AS t(`x`, `y`)"
  )
  expect_identical(
    spark_values_select(data.frame(x = integer())),
    "SELECT CAST(NULL AS INT) AS `x` WHERE 1 = 0"
  )
  expect_error(spark_values_select(data.frame()), class = "zuspark_error")
})

test_that("spark_driver() needs DBI", {
  local_mocked_bindings(dbi_ready = function() FALSE)
  expect_error(spark_driver(), class = "zuspark_error")
})

test_that("a DBI connection runs queries and fetches them in chunks", {
  skip_if_not_installed("DBI")
  local_replay("sql-query")
  con <- DBI::dbConnect(spark_driver(), con = replay_connect("sql-query"))
  expect_true(methods::is(con, "DBIConnection"))
  expect_true(DBI::dbIsValid(con))
  expect_identical(spark_version(con), "4.2.1")

  res <- DBI::dbSendQuery(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_identical(DBI::dbGetStatement(res), "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  expect_false(DBI::dbHasCompleted(res))
  first <- DBI::dbFetch(res, n = 2)
  expect_identical(first$id, c(0, 1))
  rest <- DBI::dbFetch(res)
  expect_identical(rest$id, c(2, 3, 4))
  expect_true(DBI::dbHasCompleted(res))
  expect_identical(DBI::dbGetRowCount(res), 5L)
  expect_true(DBI::dbClearResult(res))
  expect_false(DBI::dbIsValid(res))
  expect_error(DBI::dbFetch(res), class = "zuspark_error")

  expect_true(DBI::dbDisconnect(con))
  expect_false(DBI::dbIsValid(con))
})

test_that("dbGetQuery() returns a command's rows, and params are refused", {
  skip_if_not_installed("DBI")
  local_replay("sql-command")
  con <- DBI::dbConnect(spark_driver(), con = replay_connect("sql-command"))
  expect_identical(DBI::dbGetQuery(con, "SHOW DATABASES"), data.frame(namespace = "default"))
  expect_error(DBI::dbGetQuery(con, "SELECT ?", params = list(1)), class = "zuspark_error")
})

test_that("DBI quoting and types follow Spark", {
  skip_if_not_installed("DBI")
  local_replay("connect")
  con <- DBI::dbConnect(spark_driver(), con = replay_connect("connect"))
  expect_identical(as.character(DBI::dbQuoteIdentifier(con, "a b")), "`a b`")
  expect_identical(as.character(DBI::dbQuoteIdentifier(con, DBI::Id(schema = "s", table = "t"))), "`s`.`t`")
  expect_identical(as.character(DBI::dbQuoteString(con, "it's")), "'it\\'s'")
  expect_identical(as.character(DBI::dbQuoteLiteral(con, 1L)), "CAST(1 AS INT)")
  expect_identical(DBI::dbDataType(con, data.frame(a = 1L, b = "x")), c(a = "INT", b = "STRING"))
  expect_identical(DBI::dbDataType(spark_driver(), TRUE), "BOOLEAN")
  expect_error(DBI::dbBegin(con), class = "zuspark_error")
  expect_error(DBI::dbCommit(con), class = "zuspark_error")
  expect_error(DBI::dbRollback(con), class = "zuspark_error")
  expect_error(DBI::dbConnect(spark_driver(), con = list()), class = "zuspark_error")
  expect_identical(DBI::dbGetInfo(con)$db.version, "4.2.1")
})

test_that("dbplyr translates for a zuspark connection with its Spark SQL backend", {
  skip_if_not_installed("DBI")
  skip_if_not_installed("dbplyr")
  skip_if_not_installed("dplyr")
  local_replay("connect")
  con <- DBI::dbConnect(spark_driver(), con = replay_connect("connect"))
  lazy <- dbplyr::tbl_lazy(data.frame(day = 1L, amount = 1), con = con, name = "sales")
  q <- dplyr::summarise(
    dplyr::group_by(dplyr::filter(lazy, amount > 2), day),
    n = dplyr::n(), med = stats::median(amount, na.rm = TRUE)
  )
  sql <- as.character(dbplyr::sql_render(q))
  expect_match(sql, "FROM `sales`", fixed = TRUE)
  expect_match(sql, "MEDIAN(`amount`)", fixed = TRUE)
  expect_identical(dbplyr::dbplyr_edition(con), 2L)
  expect_match(dbplyr::db_connection_describe(con), "^Spark 4.2.1 at ")
})
