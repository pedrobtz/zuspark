# The type matrix of design §7.7: tests/testthat/fixtures/types/types.csv
# lists one query per type; tools/pyspark-types.py recorded PySpark's Arrow
# bytes for each (<name>.arrows), except the types PySpark itself cannot
# give as Arrow (unsupported.csv).

type_matrix <- function() {
  utils::read.csv(test_path("fixtures", "types", "types.csv"), stringsAsFactors = FALSE)
}

pyspark_refused <- function() {
  utils::read.csv(test_path("fixtures", "types", "unsupported.csv"), stringsAsFactors = FALSE)$name
}

pyspark_bytes <- function(name) {
  read_binpb(test_path("fixtures", "types", paste0(name, ".arrows")))
}

# The column `c` of an IPC stream, converted the way spark_collect() does.
convert_c <- function(bytes, lossy = "error", to = NULL) {
  arrow_call(zuarrow::arrow_to_r(read_batches(list(bytes)), to = to, lossy = lossy))$c
}

# Whether `package` has registered an exact target with zuarrow (zuint,
# decimal and zutempo had not yet on 2026-11-05: zuint#52, decimal#24).
registered_with_zuarrow <- function(package) {
  if (!requireNamespace(package, quietly = TRUE)) {
    return(FALSE)
  }
  package %in% zuarrow::arrow_registry()$package
}
