# DBI and dbplyr (design D5, amended 2026-11-07). Both are in Suggests. The
# S4 classes and DBI methods are defined in .onLoad() when DBI is
# installed; anything at top level would be frozen at install time, and
# after .onLoad() the namespace is locked, so a DBI installed later needs
# a restart. dbplyr's methods are registered into dbplyr's own namespace,
# when dbplyr is loaded, so loading zuspark never loads dbplyr. The DBI
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
#' `"character"`) for every DBI and dbplyr query, since many Spark tables
#' have `DECIMAL` or `BIGINT` columns. `dbFetch(res, n)` returns the rows in
#' chunks, but reads the whole result from the server on its first call.
#'
#' `dbWriteTable()`, `dbAppendTable()` and `copy_to()` send the rows as SQL
#' literals, which suits small tables: they warn above
#' `getOption("zuspark.write_warn_rows", 10000)` rows. A temporary table is
#' a temporary view of the session. Spark has no transactions, so
#' `dbBegin()` is an error, and it does not report rows affected, so
#' `dbExecute()` returns `NA`.
#'
#' @return A `ZusparkDriver` object.
#' @export
#' @examplesIf nzchar(Sys.getenv("SPARK_REMOTE")) && requireNamespace("DBI", quietly = TRUE)
#' con <- DBI::dbConnect(spark_driver())
#' DBI::dbGetQuery(con, "SELECT id, id * 2 AS twice FROM range(3)")
#' small <- data.frame(x = 1:3, y = c("a", "b", "c"))
#' DBI::dbWriteTable(con, "small", small, temporary = TRUE)
#' DBI::dbListTables(con)
#' DBI::dbDisconnect(con)
spark_driver <- function() {
  if (!dbi_ready()) {
    why <- if (isNamespaceLoaded("DBI")) {
      dbi_state$error %||% "DBI was installed after zuspark was loaded: restart R."
    } else {
      "install.packages(\"DBI\")."
    }
    zuspark_abort("zuspark_error", paste("`spark_driver()` needs the DBI package:", why))
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
  if (!length(x)) {
    return(character())
  }
  if (anyNA(x)) {
    zuspark_abort("zuspark_error", "An identifier can't be NA.")
  }
  paste0("`", gsub("`", "``", x, fixed = TRUE), "`")
}
spark_quote_string <- function(x) {
  if (!length(x)) {
    return(character())
  }
  out <- paste0("'", gsub("'", "\\'", gsub("\\", "\\\\", x, fixed = TRUE), fixed = TRUE), "'")
  out[is.na(x)] <- "NULL"
  out
}

# Spark's SQL type for an R vector.
spark_data_type <- function(x) {
  if (inherits(x, "integer64")) return("BIGINT")
  if (inherits(x, "POSIXct")) return("TIMESTAMP")
  if (inherits(x, "Date")) return("DATE")
  if (inherits(x, c("difftime", "hms"))) {
    zuspark_abort("zuspark_error", "Durations and times of day can't be written yet; convert them to numbers or text.")
  }
  if (is.factor(x)) return("STRING")
  if (is.logical(x)) return("BOOLEAN")
  if (is.integer(x)) return("INT")
  if (is.numeric(x)) return("DOUBLE")
  if (is.character(x)) return("STRING")
  if (is.raw(x) || inherits(x, "blob") || is.list(x)) return("BINARY")
  zuspark_abort("zuspark_error", sprintf("No Spark type for an R %s.", class(x)[[1L]]))
}

# One column's values as Spark SQL literals, cast to `type`.
spark_literals <- function(x, type = spark_data_type(x)) {
  missing <- if (is.list(x)) vapply(x, is.null, logical(1)) else is.na(x)
  text <- if (is.factor(x) || is.character(x)) {
    spark_quote_string(as.character(x))
  } else if (is.logical(x)) {
    ifelse(x, "TRUE", "FALSE")
  } else if (inherits(x, "POSIXct")) {
    # An instant, not wall-clock text: Spark reads a zone-less timestamp in
    # the session's time zone.
    sprintf("timestamp_micros(%.0f)", round(as.numeric(x) * 1e6))
  } else if (inherits(x, "Date")) {
    spark_quote_string(format(x, "%Y-%m-%d"))
  } else if (inherits(x, "integer64")) {
    as.character(x)
  } else if (is.raw(x) || inherits(x, "blob") || is.list(x)) {
    vapply(x, function(b) {
      if (is.null(b)) "" else paste0("X'", paste(format(as.raw(b)), collapse = ""), "'")
    }, character(1))
  } else if (is.integer(x)) {
    as.character(x)
  } else {
    # One value at a time: format() would choose one notation for all.
    out <- sprintf("%.17g", x)
    out[is.nan(x)] <- "'NaN'"
    out[x %in% Inf] <- "'Infinity'"
    out[x %in% -Inf] <- "'-Infinity'"
    missing <- is.na(x) & !is.nan(x)
    out
  }
  text[missing] <- "NULL"
  sprintf("CAST(%s AS %s)", text, type)
}

# A data frame as a SELECT: its rows as VALUES, or no rows with its types.
# `types` overrides the Spark type of named columns.
spark_values_select <- function(df, types = NULL) {
  if (!ncol(df)) {
    zuspark_abort("zuspark_error", "A table needs at least one column.")
  }
  names <- spark_quote_identifier(names(df))
  col_types <- vapply(df, spark_data_type, character(1))
  if (length(types)) {
    unknown <- setdiff(names(types), names(df))
    if (is.null(names(types)) || length(unknown)) {
      zuspark_abort("zuspark_error", "`field.types` must be named by columns of `value`.")
    }
    col_types[names(types)] <- types
  }
  if (!nrow(df)) {
    cols <- sprintf("CAST(NULL AS %s) AS %s", col_types, names)
    return(sprintf("SELECT %s WHERE 1 = 0", paste(cols, collapse = ", ")))
  }
  literals <- Map(spark_literals, df, col_types)
  rows <- do.call(paste, c(unname(literals), sep = ", "))
  sprintf(
    "SELECT * FROM VALUES %s AS t(%s)",
    paste0("(", rows, ")", collapse = ", "),
    paste(names, collapse = ", ")
  )
}

dbi_table_name <- function(conn, name) {
  as.character(DBI::dbQuoteIdentifier(conn, name))
}

# The data frame to write: row names as a column if asked, and a warning
# when it is large for SQL literals.
dbi_write_value <- function(value, row.names) {
  value <- DBI::sqlRownamesToColumn(value, row.names)
  limit <- getOption("zuspark.write_warn_rows", 10000)
  if (nrow(value) > limit) {
    warning(
      sprintf("Writing %d rows as SQL literals is slow; zuspark is meant for small tables here.", nrow(value)),
      call. = FALSE
    )
  }
  value
}

# DBI's arguments that other drivers take and zuspark doesn't: refused
# rather than sent to the server as headers.
dbi_foreign_args <- c("bigint", "timezone", "timezone_out", "dbname", "host", "port", "user", "password", "drv")

register_dbi <- function(ns) {
  if (dbi_ready() || !requireNamespace("DBI", quietly = TRUE)) {
    return(invisible())
  }
  # A failure here must not make zuspark fail to load: the spark_*() verbs
  # don't need DBI. spark_driver() reports it.
  tryCatch(
    {
      define_dbi(ns)
      dbi_state$dbi <- TRUE
    },
    error = function(e) {
      dbi_state$error <- conditionMessage(e)
      warning(sprintf("zuspark's DBI driver is unavailable: %s", conditionMessage(e)), call. = FALSE)
    }
  )
  invisible()
}

define_dbi <- function(ns) {
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
  methods::setClass("ZusparkDriver", contains = "DBIDriver", where = ns)
  methods::setClass("ZusparkConnection", contains = "DBIConnection", slots = c(con = "ANY"), where = ns)
  methods::setClass("ZusparkResult", contains = "DBIResult", slots = c(state = "environment"), where = ns)

  data_type <- function(dbObj, obj, ...) {
    if (is.data.frame(obj)) vapply(obj, spark_data_type, character(1)) else spark_data_type(obj)
  }
  setMethod(methods::show, "ZusparkDriver", function(object) cat("<ZusparkDriver>\n"))
  setMethod(methods::show, "ZusparkConnection", function(object) print(object@con))
  setMethod(DBI::dbGetInfo, "ZusparkDriver", function(dbObj, ...) {
    list(driver.version = utils::packageVersion("zuspark"), client.version = descriptor_version()[["spark"]])
  })
  setMethod(DBI::dbIsValid, "ZusparkDriver", function(dbObj, ...) TRUE)
  setMethod(DBI::dbDataType, "ZusparkDriver", data_type)

  setMethod(DBI::dbConnect, "ZusparkDriver", function(drv, url = Sys.getenv("SPARK_REMOTE"), ..., con = NULL) {
    foreign <- intersect(names(list(...)), dbi_foreign_args)
    if (length(foreign)) {
      zuspark_abort(
        "zuspark_error",
        sprintf("`dbConnect()` takes no `%s` for Spark: see `?spark_connect` for what it takes.", foreign[[1L]])
      )
    }
    con <- con %||% spark_connect(url, ...)
    if (!inherits(con, "zuspark_connection")) {
      zuspark_abort("zuspark_error", "`con` must be a connection from `spark_connect()`.")
    }
    methods::new("ZusparkConnection", con = con)
  })
  setMethod(DBI::dbDisconnect, "ZusparkConnection", function(conn, ...) {
    if (isTRUE(conn@con$released)) {
      warning("The connection is already disconnected.", call. = FALSE)
      return(invisible(TRUE))
    }
    invisible(spark_disconnect(conn@con, ...))
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
  setMethod(DBI::dbDataType, "ZusparkConnection", data_type)
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

  new_result <- function(conn, statement, lossy) {
    con <- conn@con
    # DBI allows one open result per connection: the stale one is closed.
    if (!is.null(con$dbi_result) && !con$dbi_result$cleared) {
      warning("Closing the open result of this connection.", call. = FALSE)
      con$dbi_result$cleared <- TRUE
    }
    state <- new.env(parent = emptyenv())
    state$conn <- conn
    state$statement <- statement
    state$lossy <- dbi_lossy(lossy)
    state$fetched <- 0L
    state$cleared <- FALSE
    state$is_statement <- FALSE
    con$dbi_result <- state
    state
  }
  setMethod(DBI::dbSendQuery, c("ZusparkConnection", "character"), function(conn, statement, ..., lossy = NULL) {
    check_no_params(...)
    state <- new_result(conn, statement, lossy)
    state$x <- spark_sql(conn@con, statement)
    state$data <- if (is.data.frame(state$x)) state$x else NULL
    methods::new("ZusparkResult", state = state)
  })
  setMethod(DBI::dbSendStatement, c("ZusparkConnection", "character"), function(conn, statement, ...) {
    check_no_params(...)
    state <- new_result(conn, statement, NULL)
    spark_execute(conn@con, statement)
    state$x <- NULL
    state$data <- data.frame()
    state$is_statement <- TRUE
    methods::new("ZusparkResult", state = state)
  })
  setMethod(DBI::dbExecute, c("ZusparkConnection", "character"), function(conn, statement, ...) {
    check_no_params(...)
    spark_execute(conn@con, statement)
    # Spark does not report rows affected; DBI's value for unknown.
    NA_integer_
  })

  valid_state <- function(res) {
    state <- res@state
    if (state$cleared) {
      zuspark_abort("zuspark_error", "This result was cleared.")
    }
    state
  }
  setMethod(DBI::dbFetch, "ZusparkResult", function(res, n = -1, ...) {
    state <- valid_state(res)
    if (!is.numeric(n) || length(n) != 1L || is.na(n) || (n != -1 && n < 0) ||
      (is.finite(n) && n != round(n))) {
      zuspark_abort("zuspark_error", "`n` must be a whole number, -1 or Inf.")
    }
    if (state$is_statement) {
      warning("A statement has no rows to fetch.", call. = FALSE)
    }
    if (is.null(state$data)) {
      x <- state$x
      state$data <- tryCatch(
        spark_collect(x, lossy = state$lossy),
        # No rows, nothing to lose: dbplyr reads a table's columns this
        # way (WHERE 0 = 1), and a DECIMAL column must not stop it.
        zuarrow_lossy_error = function(e) {
          if (identical(x$rows, 0)) spark_collect(x, lossy = "allow") else stop(e)
        }
      )
    }
    total <- nrow(state$data)
    from <- state$fetched
    take <- if (n < 0 || is.infinite(n)) total - from else min(n, total - from)
    rows <- state$data[from + seq_len(take), , drop = FALSE]
    rownames(rows) <- NULL
    state$fetched <- from + take
    rows
  })
  setMethod(DBI::dbHasCompleted, "ZusparkResult", function(res, ...) {
    state <- valid_state(res)
    state$is_statement || (!is.null(state$data) && state$fetched >= nrow(state$data))
  })
  setMethod(DBI::dbClearResult, "ZusparkResult", function(res, ...) {
    if (res@state$cleared) {
      warning("This result was already cleared.", call. = FALSE)
    }
    res@state$cleared <- TRUE
    invisible(TRUE)
  })
  setMethod(DBI::dbIsValid, "ZusparkResult", function(dbObj, ...) !dbObj@state$cleared)
  setMethod(DBI::dbGetStatement, "ZusparkResult", function(res, ...) valid_state(res)$statement)
  setMethod(DBI::dbGetRowCount, "ZusparkResult", function(res, ...) as.integer(valid_state(res)$fetched))
  setMethod(DBI::dbGetRowsAffected, "ZusparkResult", function(res, ...) {
    if (valid_state(res)$is_statement) NA_integer_ else 0L
  })
  setMethod(DBI::dbColumnInfo, "ZusparkResult", function(res, ...) {
    state <- valid_state(res)
    if (inherits(state$x, "zuspark_query")) {
      schema <- spark_schema(state$x)
      return(data.frame(name = schema$name, type = schema$type, stringsAsFactors = FALSE))
    }
    df <- state$data %||% data.frame()
    types <- tolower(vapply(df, spark_data_type, character(1)))
    data.frame(name = names(df), type = unname(types), stringsAsFactors = FALSE)
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
    if_exists <- if (fail_if_missing) "" else "IF EXISTS "
    # DROP TABLE drops a temporary view too.
    spark_execute(conn@con, paste0("DROP TABLE ", if_exists, dbi_table_name(conn, name)))
    invisible(TRUE)
  })
  set_name_method(DBI::dbWriteTable, extra = "data.frame", function(conn, name, value, ..., row.names = FALSE, overwrite = FALSE, append = FALSE, field.types = NULL, temporary = FALSE) {
    if (overwrite && append) {
      zuspark_abort("zuspark_error", "Choose `overwrite` or `append`, not both.")
    }
    if (append && length(field.types)) {
      zuspark_abort("zuspark_error", "`field.types` can't be given with `append = TRUE`.")
    }
    value <- dbi_write_value(value, row.names)
    table <- dbi_table_name(conn, name)
    exists <- DBI::dbExistsTable(conn, name)
    if (exists && !overwrite && !append) {
      zuspark_abort("zuspark_error", sprintf("Table %s exists: pass `overwrite = TRUE` or `append = TRUE`.", table))
    }
    if (append && exists) {
      if (temporary) {
        zuspark_abort("zuspark_error", "A temporary table is a view, which can't be appended to.")
      }
      dbi_insert(conn, table, value)
    } else if (temporary) {
      spark_execute(conn@con, sprintf(
        "CREATE OR REPLACE TEMPORARY VIEW %s AS %s", table, spark_values_select(value, field.types)
      ))
    } else {
      # Spark's session catalog has no CREATE OR REPLACE TABLE ... AS for
      # its default tables, so an overwrite drops first.
      if (exists) spark_execute(conn@con, paste("DROP TABLE", table))
      spark_execute(conn@con, sprintf("CREATE TABLE %s AS %s", table, spark_values_select(value, field.types)))
    }
    invisible(TRUE)
  })
  set_name_method(DBI::dbAppendTable, extra = "data.frame", function(conn, name, value, ..., row.names = NULL) {
    if (!is.null(row.names)) {
      zuspark_abort("zuspark_error", "`dbAppendTable()` takes no `row.names`; DBI requires it to be NULL.")
    }
    value <- dbi_write_value(value, FALSE)
    dbi_insert(conn, dbi_table_name(conn, name), value)
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
  invisible()
}

# Rows go in by column name, as DBI requires, not by position.
dbi_insert <- function(conn, table, value) {
  columns <- paste(spark_quote_identifier(names(value)), collapse = ", ")
  spark_execute(conn@con, sprintf("INSERT INTO %s (%s) %s", table, columns, spark_values_select(value)))
}

check_no_params <- function(...) {
  args <- list(...)
  if ("params" %in% names(args) && length(args$params)) {
    zuspark_abort("zuspark_error", "Parameterised queries are not supported yet; put the values in the SQL.")
  }
  invisible()
}

# dbplyr's own 'Spark SQL' backend, for zuspark's connection: every method
# dbplyr has for it (the SQL translation, how copy_to() and compute()
# write), registered into dbplyr's namespace. Called when dbplyr is loaded,
# never loading it.
register_dbplyr <- function() {
  if (isTRUE(dbi_state$dbplyr) || !dbi_ready() || !isNamespaceLoaded("dbplyr")) {
    return(invisible())
  }
  ns <- asNamespace("dbplyr")
  table <- get(".__S3MethodsTable__.", envir = ns)
  suffix <- ".Spark SQL"
  for (method in ls(table)) {
    if (endsWith(method, suffix)) {
      generic <- substr(method, 1L, nchar(method) - nchar(suffix))
      registerS3method(generic, "ZusparkConnection", table[[method]], envir = ns)
    }
  }
  if (is.null(table[["dbplyr_edition.Spark SQL"]])) {
    registerS3method("dbplyr_edition", "ZusparkConnection", function(con) 2L, envir = ns)
  }
  # copy_to(temporary = FALSE) and compute(temporary = FALSE) with
  # overwrite: Spark's session catalog has no CREATE OR REPLACE TABLE for
  # its default tables, so the table is dropped first.
  spark_compute <- table[["db_compute.Spark SQL"]]
  if (!is.null(spark_compute)) {
    registerS3method("db_compute", "ZusparkConnection", function(con, table, sql, ..., overwrite = FALSE, temporary = TRUE) {
      if (!temporary && overwrite) {
        DBI::dbRemoveTable(con, DBI::SQL(as.character(table)), fail_if_missing = FALSE)
        overwrite <- FALSE
      }
      spark_compute(con, table, sql, ..., overwrite = overwrite, temporary = temporary)
    }, envir = ns)
  }
  registerS3method("db_connection_describe", "ZusparkConnection", function(con, ...) {
    sprintf("Spark %s at %s", con@con$spark_version %||% "?", con@con$url)
  }, envir = ns)
  dbi_state$dbplyr <- TRUE
  invisible()
}
