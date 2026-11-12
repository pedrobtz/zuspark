.libPaths(c("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/lib", .libPaths()))
library(zuspark)
fx <- "/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/src/tests/testthat/fixtures"
rd <- function(p) readBin(p, "raw", file.size(p))
pool <- zuspark:::spark_pool()
for (sc in c("sql-query","sql-command")) for (f in list.files(file.path(fx, sc), full.names=TRUE)) {
  m <- sub("^[0-9]+-([A-Za-z]+)\\..*", "\\1", basename(f))
  type <- if (grepl("request", f)) paste0("spark.connect.", m, "Request") else if (m %in% c("ExecutePlan","ReattachExecute")) "spark.connect.ExecutePlanResponse" else paste0("spark.connect.", m, "Response")
  x <- zuproto::proto_decode(rd(f), type, pool, enums="character")
  cat("==", sc, basename(f), "\n"); str(x, max.level=4, vec.len=3, nchar.max=60)
}
