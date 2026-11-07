# DBI and dbplyr (design D5, amended 2026-11-07). Both are in Suggests. The
# S4 classes and DBI methods are defined when zuspark loads, and only if
# DBI is installed; anything at top level would be frozen at install time.
# If DBI or dbplyr is loaded later, a hook registers them then. The DBI
# connection wraps the same zuspark_connection the spark_*() verbs use.

#' A DBI driver for Spark Connect
#'
#' `spark_driver()` returns the driver to give [DBI::dbConnect()], so that
#' a Spark Connect session can be used through DBI and dbplyr:
#' `DBI::dbGetQuery()`, `DBI::dbListTables()`, `dplyr::tbl()`,
#' `dplyr::collect()`, `dplyr::copy_to()` and the rest. It needs the DBI
#' package; dbplyr, for `tbl()`, uses its own 'Spark SQL' translation.
#'
#' `dbConnect(spark_driver(), url, ...)` passes `url` and `...` to
#' [spark_connect()]; `dbConnect(spark_driver(), con = con)` wraps a
#' connection made already, such as one from [spark_connect_databricks()].
#' Every `spark_*()` function also accepts the DBI connection.
#'
#' Results follow zuspark's types: a value R cannot hold exactly is an error
#' unless `lossy` says otherwise. Pass `lossy` to `dbGetQuery()` or
#' `dbSendQuery()`, or set `options(zuspark.lossy = "allow")` (or
#' `"character"`) for every DBI and dbplyr query.
#'
#' `dbWriteTable()` and `copy_to()` send the data as SQL literals, which
#' suits small tables; a temporary table is a temporary view of the
#' session. Spark has no transactions, so `dbBegin()` is an error.
#' `dbExecute()` returns the number of rows the statement produced, since
#' Spark does not report rows affected.
#'
#' @return A `ZusparkDriver` object.
#' @export
#' @examplesIf nzchar(Sys.getenv("SPARK_REMOTE")) && requireNamespace("DBI", quietly = TRUE)
#' con <- DBI::dbConnect(spark_driver())
#' DBI::dbGetQuery(con, "SELECT id, id * 2 AS twice FROM range(3)")
#' DBI::dbWriteTable(con, "small", data.frame(x = 1:3, y = c("a", "b", "c")), temporary = TRUE)
#' DBI::dbListTables(con)
#' DBI::dbDisconnect(con)
spark_driver <- function() {
  if (!dbi_ready()) {
    zuspark_abort("zuspark_error", "`spark_driver()` needs the DBI package: install.packages(\"DBI\").")
  }
  methods::new("ZusparkDriver")
}

dbi_state <- new.env(parent = emptyenv())
dbi_ready <- function() isTRUE(dbi_state$dbi)

# A zuspark_connection, or the one inside a DBI connection.
unwrap_con <- function(con) {
  if (isS4(con) && methods::is(con, "ZusparkConnection")) con@con else con
}

dbi_lossy <- function(lossy = NULL) lossy %||% getOption("zuspark.lossy", "error")

# Spark SQL's quoting: identifiers in backticks, strings in single quotes
# with backslash escapes (spark.sql.parser.escapedStringLiterals is off by
# default, so a backslash escapes and '' would be two literals).
spark_quote_identifier <- function(x) {
  paste0("`", gsub("`", "``", x, fixed = TRUE), "`")
}
spark_quote_string <- function(x) {
  out <- paste0("'", gsub("'", "\\'", gsub("\\", "\\\\", x, fixed = TRUE), fixed = TRUE), "'")
  out[is.na(x)] <- "NULL"
  out
}

# Spark's SQL type for an R vector.
spark_data_type <- function(x) {
  if (inherits(x, "POSIXct")) return("TIMESTAMP")
  if (inherits(x, "Date")) return("DATE")
  if (is.factor(x)) return("STRING")
  if (is.logical(x)) return("BOOLEAN")
  if (is.integer(x)) return("INT")
  if (is.numeric(x)) return("DOUBLE")
  if (is.character(x)) return("STRING")
  if (is.raw(x) || inherits(x, "blob") || is.list(x)) return("BINARY")
  zuspark_abort("zuspark_error", sprintf("No Spark type for an R %s.", class(x)[[1L]]))
}

# One column's values as Spark SQL literals, cast to its type.
spark_literals <- function(x) {
  type <- spark_data_type(x)
  text <- if (is.factor(x) || is.character(x)) {
    spark_quote_string(as.character(x))
  } else if (is.logical(x)) {
    ifelse(x, "TRUE", "FALSE")
  } else if (inherits(x, "POSIXct")) {
    spark_quote_string(format(x, "%Y-%m-%d %H:%M:%OS6", tz = "UTC"))
  } else if (inherits(x, "Date")) {
    spark_quote_string(format(x, "%Y-%m-%d"))
  } else if (type == "BINARY") {
    vapply(x, function(b) paste0("X'", paste(format(as.raw(b)), collapse = ""), "'"), character(1))
  } else {
    format(x, scientific = FALSE, digits = 17, trim = TRUE)
  }
  text[is.na(x)] <- "NULL"
  sprintf("CAST(%s AS %s)", text, type)
}

# A data frame as a SELECT: its rows as VALUES, or no rows with its types.
spark_values_select <- function(df) {
  names <- spark_quote_identifier(names(df))
  if (!ncol(df)) {
    zuspark_abort("zuspark_error", "A table needs at least one column.")
  }
  if (!nrow(df)) {
    cols <- vapply(seq_along(df), function(j) {
      sprintf("CAST(NULL AS %s) AS %s", spark_data_type(df[[j]]), names[[j]])
    }, character(1))
    return(sprintf("SELECT %s WHERE 1 = 0", paste(cols, collapse = ", ")))
  }
  literals <- lapply(df, spark_literals)
  rows <- do.call(paste, c(literals, sep = ", "))
  sprintf(
    "SELECT * FROM VALUES %s AS t(%s)",
    paste0("(", rows, ")", collapse = ", "),
    paste(names, collapse = ", ")
  )
}

dbi_table_name <- function(conn, name) {
  as.character(DBI::dbQuoteIdentifier(conn, name))
}

# Defines the classes and methods once DBI is available.
register_dbi <- function(ns) {
  if (dbi_ready() || !requireNamespace("DBI", quietly = TRUE)) {
    return(invisible())
  }
  setClass <- methods::setClass
  setMethod <- function(f, signature, definition) {
    methods::setMethod(f, signature, definition, where = ns)
  }
  # Table names come as a string, an Id or SQL: one method for each, so
  # that none of DBI's own DBIConnection methods is a closer match.
  set_name_method <- function(f, definition, extra = NULL) {
    for (name_class in c("character", "Id", "SQL")) {
      setMethod(f, c("ZusparkConnection", name_class, extra), definition)
    }
  }
  setClass("ZusparkDriver", contains = "DBIDriver", where = ns)
  setClass("ZusparkConnection", contains = "DBIConnection", slots = c(con = "ANY"), where = ns)
  setClass("ZusparkResult", contains = "DBIResult", slots = c(state = "environment"), where = ns)

  setMethod(methods::show, "ZusparkDriver", function(object) cat("<ZusparkDriver>\n"))
  setMethod(methods::show, "ZusparkConnection", function(object) print(object@con))
  setMethod(DBI::dbGetInfo, "ZusparkDriver", function(dbObj, ...) {
    list(driver.version = utils::packageVersion("zuspark"), client.version = descriptor_version()[["spark"]])
  })
  setMethod(DBI::dbIsValid, "ZusparkDriver", function(dbObj, ...) TRUE)
  setMethod(DBI::dbDataType, "ZusparkDriver", function(dbObj, obj, ...) {
    if (is.data.frame(obj)) vapply(obj, spark_data_type, character(1)) else spark_data_type(obj)
  })

  setMethod(DBI::dbConnect, "ZusparkDriver", function(drv, url = Sys.getenv("SPARK_REMOTE"), ..., con = NULL) {
    con <- con %||% spark_connect(url, ...)
    if (!inherits(con, "zuspark_connection")) {
      zuspark_abort("zuspark_error", "`con` must be a connection from `spark_connect()`.")
    }
    methods::new("ZusparkConnection", con = con)
  })
  setMethod(DBI::dbDisconnect, "ZusparkConnection", function(conn, ...) {
    spark_disconnect(conn@con, ...)
  })
  setMethod(DBI::dbIsValid, "ZusparkConnection", function(dbObj, ...) {
    !isTRUE(dbObj@con$released)
  })
  setMethod(DBI::dbGetInfo, "ZusparkConnection", function(dbObj, ...) {
    con <- dbObj@con
    list(
      dbname = "spark", db.version = con$spark_version, url = con$url,
      session_id = con$session_id, username = con$user_id
    )
  })
  setMethod(DBI::dbDataType, "ZusparkConnection", function(dbObj, obj, ...) {
    if (is.data.frame(obj)) vapply(obj, spark_data_type, character(1)) else spark_data_type(obj)
  })
  setMethod(DBI::dbQuoteIdentifier, c("ZusparkConnection", "character"), function(conn, x, ...) {
    DBI::SQL(spark_quote_identifier(x), names = names(x))
  })
  setMethod(DBI::dbQuoteIdentifier, c("ZusparkConnection", "SQL"), function(conn, x, ...) x)
  setMethod(DBI::dbQuoteIdentifier, c("ZusparkConnection", "Id"), function(conn, x, ...) {
    DBI::SQL(paste(spark_quote_identifier(x@name), collapse = "."))
  })
  setMethod(DBI::dbQuoteString, c("ZusparkConnection", "character"), function(conn, x, ...) {
    DBI::SQL(spark_quote_string(x), names = names(x))
  })
  setMethod(DBI::dbQuoteString, c("ZusparkConnection", "SQL"), function(conn, x, ...) x)
  setMethod(DBI::dbQuoteLiteral, "ZusparkConnection", function(conn, x, ...) {
    if (methods::is(x, "SQL")) return(x)
    DBI::SQL(spark_literals(x))
  })

  setMethod(DBI::dbSendQuery, c("ZusparkConnection", "character"), function(conn, statement, ..., lossy = NULL) {
    check_no_params(...)
    state <- new.env(parent = emptyenv())
    state$conn <- conn
    state$statement <- statement
    state$lossy <- dbi_lossy(lossy)
    state$x <- spark_sql(conn@con, statement)
    state$data <- if (is.data.frame(state$x)) state$x else NULL
    state$fetched <- 0L
    state$cleared <- FALSE
    state$rows_affected <- NA_integer_
    methods::new("ZusparkResult", state = state)
  })
  setMethod(DBI::dbSendStatement, c("ZusparkConnection", "character"), function(conn, statement, ...) {
    check_no_params(...)
    state <- new.env(parent = emptyenv())
    state$conn <- conn
    state$statement <- statement
    state$lossy <- dbi_lossy()
    state$x <- NULL
    rows <- spark_execute(conn@con, statement)
    state$data <- data.frame()
    state$fetched <- 0L
    state$cleared <- FALSE
    state$rows_affected <- as.integer(rows)
    methods::new("ZusparkResult", state = state)
  })
  setMethod(DBI::dbExecute, c("ZusparkConnection", "character"), function(conn, statement, ...) {
    check_no_params(...)
    as.integer(spark_execute(conn@con, statement))
  })
  setMethod(DBI::dbFetch, "ZusparkResult", function(res, n = -1, ...) {
    state <- res@state
    if (state$cleared) {
      zuspark_abort("zuspark_error", "This result was cleared.")
    }
    if (is.null(state$data)) {
      state$data <- spark_collect(state$x, lossy = state$lossy)
    }
    total <- nrow(state$data)
    from <- state$fetched
    take <- if (is.infinite(n) || n < 0) total - from else min(n, total - from)
    rows <- state$data[from + seq_len(take), , drop = FALSE]
    rownames(rows) <- NULL
    state$fetched <- from + take
    rows
  })
  setMethod(DBI::dbHasCompleted, "ZusparkResult", function(res, ...) {
    state <- res@state
    !is.null(state$data) && state$fetched >= nrow(state$data)
  })
  setMethod(DBI::dbClearResult, "ZusparkResult", function(res, ...) {
    res@state$cleared <- TRUE
    invisible(TRUE)
  })
  setMethod(DBI::dbIsValid, "ZusparkResult", function(dbObj, ...) !dbObj@state$cleared)
  setMethod(DBI::dbGetStatement, "ZusparkResult", function(res, ...) res@state$statement)
  setMethod(DBI::dbGetRowCount, "ZusparkResult", function(res, ...) as.integer(res@state$fetched))
  setMethod(DBI::dbGetRowsAffected, "ZusparkResult", function(res, ...) res@state$rows_affected)
  setMethod(DBI::dbColumnInfo, "ZusparkResult", function(res, ...) {
    state <- res@state
    if (inherits(state$x, "zuspark_query")) {
      schema <- spark_schema(state$x)
      return(data.frame(name = schema$name, type = schema$type, stringsAsFactors = FALSE))
    }
    df <- state$data %||% data.frame()
    data.frame(name = names(df), type = vapply(df, function(v) class(v)[[1L]], ""), stringsAsFactors = FALSE)
  })

  setMethod(DBI::dbListTables, "ZusparkConnection", function(conn, ...) {
    tables <- spark_sql(conn@con, "SHOW TABLES")
    as.character(tables$tableName)
  })
  set_name_method(DBI::dbExistsTable, function(conn, name, ...) {
    sql <- paste("SELECT * FROM", dbi_table_name(conn, name))
    tryCatch(
      {
        spark_schema(spark_sql(conn@con, sql))
        TRUE
      },
      zuspark_sql_error = function(e) {
        if (identical(e$error_class, "TABLE_OR_VIEW_NOT_FOUND")) FALSE else stop(e)
      }
    )
  })
  set_name_method(DBI::dbListFields, function(conn, name, ...) {
    sql <- paste("SELECT * FROM", dbi_table_name(conn, name))
    spark_schema(spark_sql(conn@con, sql))$name
  })
  set_name_method(DBI::dbRemoveTable, function(conn, name, ..., temporary = FALSE, fail_if_missing = TRUE) {
    if (!fail_if_missing && !DBI::dbExistsTable(conn, name)) {
      return(invisible(TRUE))
    }
    # DROP TABLE drops a temporary view too.
    spark_execute(conn@con, paste("DROP TABLE", dbi_table_name(conn, name)))
    invisible(TRUE)
  })
  set_name_method(DBI::dbWriteTable, extra = "data.frame", function(conn, name, value, ..., overwrite = FALSE, append = FALSE, temporary = FALSE) {
    if (overwrite && append) {
      zuspark_abort("zuspark_error", "Choose `overwrite` or `append`, not both.")
    }
    table <- dbi_table_name(conn, name)
    select <- spark_values_select(value)
    exists <- DBI::dbExistsTable(conn, name)
    if (exists && !overwrite && !append) {
      zuspark_abort("zuspark_error", sprintf("Table %s exists: pass `overwrite = TRUE` or `append = TRUE`.", table))
    }
    if (temporary) {
      if (append && exists) {
        zuspark_abort("zuspark_error", "A temporary table is a view, which can't be appended to.")
      }
      spark_execute(conn@con, sprintf("CREATE OR REPLACE TEMPORARY VIEW %s AS %s", table, select))
    } else if (append && exists) {
      spark_execute(conn@con, sprintf("INSERT INTO %s %s", table, select))
    } else {
      action <- if (exists) "CREATE OR REPLACE TABLE" else "CREATE TABLE"
      spark_execute(conn@con, sprintf("%s %s AS %s", action, table, select))
    }
    invisible(TRUE)
  })
  set_name_method(DBI::dbAppendTable, extra = "data.frame", function(conn, name, value, ..., row.names = NULL) {
    spark_execute(conn@con, sprintf("INSERT INTO %s %s", dbi_table_name(conn, name), spark_values_select(value)))
    invisible(nrow(value))
  })
  setMethod(DBI::dbBegin, "ZusparkConnection", function(conn, ...) {
    zuspark_abort("zuspark_error", "Spark has no transactions.")
  })
  setMethod(DBI::dbCommit, "ZusparkConnection", function(conn, ...) {
    zuspark_abort("zuspark_error", "Spark has no transactions.")
  })
  setMethod(DBI::dbRollback, "ZusparkConnection", function(conn, ...) {
    zuspark_abort("zuspark_error", "Spark has no transactions.")
  })
  dbi_state$dbi <- TRUE
  invisible()
}

check_no_params <- function(...) {
  args <- list(...)
  if ("params" %in% names(args) && length(args$params)) {
    zuspark_abort("zuspark_error", "Parameterised queries are not supported yet; put the values in the SQL.")
  }
  invisible()
}

# dbplyr's own 'Spark SQL' backend, for zuspark's connection: the SQL
# translation and how copy_to() and compute() write.
register_dbplyr <- function() {
  if (isTRUE(dbi_state$dbplyr) || !dbi_ready() || !requireNamespace("dbplyr", quietly = TRUE)) {
    return(invisible())
  }
  ns <- asNamespace("dbplyr")
  table <- get(".__S3MethodsTable__.", envir = ns)
  for (generic in c("sql_dialect", "db_compute", "db_copy_to", "dbplyr_edition")) {
    method <- table[[paste0(generic, ".Spark SQL")]]
    if (!is.null(method)) {
      registerS3method(generic, "ZusparkConnection", method, envir = ns)
    }
  }
  registerS3method("db_connection_describe", "ZusparkConnection", function(con, ...) {
    sprintf("Spark %s at %s", con@con$spark_version %||% "?", con@con$url)
  }, envir = ns)
  if (is.null(table[["dbplyr_edition.Spark SQL"]])) {
    registerS3method("dbplyr_edition", "ZusparkConnection", function(con) 2L, envir = ns)
  }
  # dbplyr before 2.6 has no sql_dialect(): it translates through
  # sql_translation().
  if (is.null(table[["sql_dialect.Spark SQL"]]) && !is.null(table[["sql_translation.Spark SQL"]])) {
    registerS3method("sql_translation", "ZusparkConnection", table[["sql_translation.Spark SQL"]], envir = ns)
  }
  dbi_state$dbplyr <- TRUE
  invisible()
}

register_dbi_dbplyr <- function(ns) {
  register_dbi(ns)
  register_dbplyr()
}
