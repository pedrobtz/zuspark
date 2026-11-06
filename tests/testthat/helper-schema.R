# Helpers and data for test-schema.R, kept out of the test file so that
# devtools::test(shuffle = TRUE) cannot run a test before them.

schema_sql <- paste(
  "SELECT 1L AS id, CAST(1.5 AS DECIMAL(10, 2)) AS amount,",
  "array(named_struct('k', 'a', 'v', map('x', 1))) AS nested,",
  "INTERVAL '1' HOUR AS h, CAST(NULL AS STRING) AS maybe"
)
