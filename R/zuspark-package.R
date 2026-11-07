#' @keywords internal
"_PACKAGE"

## usethis namespace: start
## usethis namespace: end
NULL

.onLoad <- function(libname, pkgname) {
  # DBI and dbplyr are in Suggests (R/dbi.R). The DBI classes are defined
  # now if DBI is installed: once this returns the namespace is locked, so
  # a DBI installed later needs a restart. dbplyr's methods go into
  # dbplyr's own namespace, now if it is loaded, or when it is.
  register_dbi(topenv())
  register_dbplyr()
  hook <- packageEvent("dbplyr", "onLoad")
  # One hook, whatever reloaded zuspark before (devtools::load_all()).
  kept <- Filter(function(f) !isTRUE(attr(f, "zuspark")), getHook(hook))
  setHook(hook, kept, action = "replace")
  setHook(hook, structure(function(...) register_dbplyr(), zuspark = TRUE))
}
