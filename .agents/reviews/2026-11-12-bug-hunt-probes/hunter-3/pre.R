.libPaths(c("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/lib", .libPaths()))
suppressPackageStartupMessages(library(zuspark))
URL <- "sc://127.0.0.1:16022"
say <- function(...) cat(sprintf(...), "\n")
tc <- function(expr) tryCatch(expr, condition = function(e) { cat("  <", paste(head(class(e),3), collapse="/"), "> ", conditionMessage(e), "\n", sep=""); e })
nchan <- function() length(ls(zuspark:::channels))
