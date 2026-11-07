#' @keywords internal
"_PACKAGE"

## usethis namespace: start
## usethis namespace: end
NULL

.onLoad <- function(libname, pkgname) {
  ns <- topenv()
  # DBI and dbplyr are in Suggests: their methods are defined now if they
  # are installed, or when they are loaded later (R/dbi.R).
  register_dbi_dbplyr(ns)
  setHook(packageEvent("DBI", "onLoad"), function(...) register_dbi_dbplyr(ns))
  setHook(packageEvent("dbplyr", "onLoad"), function(...) register_dbplyr())
}
