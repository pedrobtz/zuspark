# Records the `databricks` fixture the tests replay on every leg (design
# §7.8, .agents/databricks.md), from a Databricks workspace on serverless
# compute. Free Edition is enough.
#
#   DATABRICKS_HOST=https://dbc-....cloud.databricks.com \
#   DATABRICKS_TOKEN=dapi... Rscript tools/record-databricks.R
#
# The token and the headers are gRPC metadata and never reach the
# fixture, which holds only the protobuf messages. The responses do carry
# the workspace's own settings, so the script then rewrites every string
# that names the workspace or the user, and fails if one survives.

pkgload::load_all(quiet = TRUE)

host <- databricks_host(Sys.getenv("DATABRICKS_HOST"))
token <- Sys.getenv("DATABRICKS_TOKEN")
if (!nzchar(token)) stop("Set DATABRICKS_TOKEN.")
dir <- file.path("tests", "testthat", "fixtures", "databricks")

# The strings that identify the workspace or its user, and what each
# becomes in the fixture.
h <- curl::new_handle()
curl::handle_setheaders(h, Authorization = paste("Bearer", token))
res <- curl::curl_fetch_memory(sprintf("https://%s/api/2.0/preview/scim/v2/Me", host), h)
if (res$status_code != 200L) stop("The workspace refused the token: HTTP ", res$status_code, ".")
me <- jsonlite::fromJSON(rawToChar(res$content))
org <- curl::parse_headers_list(res$headers)[["x-databricks-org-id"]] %||% ""
secrets <- c(
  token = token,
  host = host,
  workspace = sub("\\..*$", "", host),
  user = me$userName %||% "",
  user_id = me$id %||% "",
  org = org
)
secrets <- secrets[nzchar(secrets)]
replacement <- c(
  token = "<token>", host = "dbc-00000000-0000.cloud.databricks.com",
  workspace = "dbc-00000000-0000", user = "user@example.com",
  user_id = "0000000000000000", org = "0000000000000000"
)

unlink(dir, recursive = TRUE)
withr::with_options(list(zuspark.record = dir), {
  con <- spark_connect_databricks(host, token, user_id = "zuspark")
  q <- spark_sql(con, "SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)")
  spark_schema(q)
  spark_collect(q)
  try(spark_sql(con, "SELECT * FROM zuspark_no_such_table"), silent = TRUE)
  spark_disconnect(con)
})

# The tests' message types (helper-fixtures.R).
sys.source(file.path("tests", "testthat", "helper-fixtures.R"), envir = environment())

# Rewrite the secrets in each message that holds one, decoded and
# re-encoded so that the lengths stay right; strings only, in any field.
# Messages without a secret keep their bytes as received.
scrub <- function(x) {
  if (is.character(x)) {
    for (k in names(secrets)) x <- gsub(secrets[[k]], replacement[[k]], x, fixed = TRUE)
    return(x)
  }
  if (is.list(x)) {
    nm <- names(x)
    x <- lapply(x, scrub)
    if (!is.null(nm)) names(x) <- scrub(nm)
  }
  x
}
holds_secret <- function(bytes) {
  any(vapply(secrets, function(s) length(grepRaw(s, bytes, fixed = TRUE)) > 0L, logical(1)))
}
for (f in list.files(dir, "\\.binpb$", full.names = TRUE)) {
  if (!holds_secret(readBin(f, "raw", file.size(f)))) next
  method <- sub("^\\d+-([A-Za-z]+)\\..*$", "\\1", basename(f))
  type <- if (grepl("\\.request\\.", f)) request_types[[method]] else response_types[[method]]
  cat("scrubbing", basename(f), "\n")
  msg <- zuproto::proto_decode(readBin(f, "raw", file.size(f)), type, spark_pool())
  writeBin(zuproto::proto_encode(scrub(msg), type, spark_pool()), f)
}

for (f in list.files(dir, "\\.binpb$", full.names = TRUE)) {
  if (holds_secret(readBin(f, "raw", file.size(f)))) {
    stop(sprintf("%s still names the workspace or its user.", basename(f)))
  }
}
cat(sprintf("databricks: %d calls, scrubbed\n", length(list.files(dir, "\\.request\\.binpb$"))))
