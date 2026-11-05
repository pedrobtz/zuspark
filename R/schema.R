#' A query's schema
#'
#' `spark_schema()` asks the server for a query's result schema, through
#' `AnalyzePlan`, without running the query. Types are Spark SQL's, as DDL
#' text: `"bigint"`, `"decimal(10,2)"`, `"array<string>"`,
#' `"struct<a:int,b:string>"`. The Arrow schema the rows arrive in, which
#' is the server's mapping of these types, is the schema of
#' [spark_fetch()]'s stream.
#'
#' @inheritParams spark_collect
#' @return A data frame with one row per column: `name`, `type` and
#'   `nullable`.
#' @export
#' @examplesIf nzchar(Sys.getenv("SPARK_REMOTE"))
#' con <- spark_connect()
#' spark_schema(spark_sql(con, "SELECT id, CAST(id AS DECIMAL(10, 2)) AS d FROM range(3)"))
#' spark_disconnect(con)
spark_schema <- function(x) {
  check_query(x)
  con <- x$con
  request <- c(session_fields(con), list(schema = list(plan = list(root = x$relation))))
  response <- spark_rpc(
    con, "AnalyzePlan", request,
    "spark.connect.AnalyzePlanRequest", "spark.connect.AnalyzePlanResponse"
  )
  observe_session(con, response)
  type <- response$schema$schema
  if (is.null(type)) {
    zuspark_abort("zuspark_protocol_error", "The server answered a schema request without a schema.")
  }
  fields <- type$struct$fields
  if (is.null(type$struct)) {
    fields <- list(list(name = "value", data_type = type, nullable = TRUE))
  }
  data.frame(
    name = vapply(fields, function(f) f$name %||% "", character(1)),
    type = vapply(fields, function(f) format_data_type(f$data_type), character(1)),
    nullable = vapply(fields, function(f) isTRUE(f$nullable), logical(1)),
    stringsAsFactors = FALSE
  )
}

day_time_fields <- c("day", "hour", "minute", "second")
year_month_fields <- c("year", "month")

# A spark.connect.DataType as Spark SQL's DDL text (DataType.sql, in lower
# case as simpleString writes it).
format_data_type <- function(type) {
  kind <- intersect(names(type), data_type_kinds)
  if (!length(kind)) {
    return("unknown")
  }
  kind <- kind[[1L]]
  t <- type[[kind]]
  switch(kind,
    null = "void",
    binary = "binary",
    boolean = "boolean",
    byte = "tinyint",
    short = "smallint",
    integer = "int",
    long = "bigint",
    float = "float",
    double = "double",
    decimal = sprintf("decimal(%d,%d)", as.integer(t$precision %||% 10), as.integer(t$scale %||% 0)),
    string = {
      collation <- t$collation %||% ""
      if (nzchar(collation) && collation != "UTF8_BINARY") {
        sprintf("string collate %s", collation)
      } else {
        "string"
      }
    },
    char = sprintf("char(%d)", as.integer(t$length %||% 1)),
    var_char = sprintf("varchar(%d)", as.integer(t$length %||% 1)),
    date = "date",
    timestamp = "timestamp",
    timestamp_ntz = "timestamp_ntz",
    time = sprintf("time(%d)", as.integer(t$precision %||% 6)),
    calendar_interval = "interval",
    year_month_interval = interval_ddl(t, year_month_fields, 0L, 1L),
    day_time_interval = interval_ddl(t, day_time_fields, 0L, 3L),
    array = sprintf("array<%s>", format_data_type(t$element_type)),
    map = sprintf("map<%s,%s>", format_data_type(t$key_type), format_data_type(t$value_type)),
    struct = sprintf(
      "struct<%s>",
      paste(vapply(t$fields, function(f) {
        paste0(f$name, ":", format_data_type(f$data_type))
      }, character(1)), collapse = ",")
    ),
    variant = "variant",
    udt = if (!is.null(t$sql_type)) format_data_type(t$sql_type) else "udt",
    geometry = sprintf("geometry(%d)", as.integer(t$srid %||% 0)),
    geography = sprintf("geography(%d)", as.integer(t$srid %||% 4326)),
    unparsed = t$data_type_string %||% "unknown",
    "unknown"
  )
}

data_type_kinds <- c(
  "null", "binary", "boolean", "byte", "short", "integer", "long", "float",
  "double", "decimal", "string", "char", "var_char", "date", "timestamp",
  "timestamp_ntz", "calendar_interval", "year_month_interval",
  "day_time_interval", "array", "struct", "map", "variant", "udt",
  "geometry", "geography", "unparsed", "time"
)

interval_ddl <- function(t, names, default_start, default_end) {
  start <- as.integer(t$start_field %||% default_start)
  end <- as.integer(t$end_field %||% default_end)
  if (start == end) {
    return(sprintf("interval %s", names[[start + 1L]]))
  }
  sprintf("interval %s to %s", names[[start + 1L]], names[[end + 1L]])
}
