# Package index

## Sessions

Open, inspect and close a Spark Connect session.

- [`spark_connect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)
  [`spark_disconnect()`](https://pedrobtz.github.io/zuspark/reference/spark_connect.md)
  : Connect to a Spark Connect server
- [`spark_connect_databricks()`](https://pedrobtz.github.io/zuspark/reference/spark_connect_databricks.md)
  : Connect to Databricks
- [`spark_version()`](https://pedrobtz.github.io/zuspark/reference/spark_version.md)
  [`spark_session_id()`](https://pedrobtz.github.io/zuspark/reference/spark_version.md)
  : Inspect a session
- [`spark_config()`](https://pedrobtz.github.io/zuspark/reference/spark_config.md)
  : Read and change the session's configuration

## SQL

Run statements and read their results.

- [`spark_sql()`](https://pedrobtz.github.io/zuspark/reference/spark_sql.md)
  [`spark_execute()`](https://pedrobtz.github.io/zuspark/reference/spark_sql.md)
  : Run SQL on the server
- [`spark_collect()`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)
  [`spark_fetch()`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)
  [`as.data.frame(`*`<zuspark_query>`*`)`](https://pedrobtz.github.io/zuspark/reference/spark_collect.md)
  : Fetch a query's result
- [`spark_schema()`](https://pedrobtz.github.io/zuspark/reference/spark_schema.md)
  : A query's schema

## DBI and dbplyr

- [`spark_driver()`](https://pedrobtz.github.io/zuspark/reference/spark_driver.md)
  : A DBI driver for Spark Connect

## Errors and interrupts

- [`spark_error_details()`](https://pedrobtz.github.io/zuspark/reference/spark_error_details.md)
  : Spark's full account of an error
- [`spark_interrupt()`](https://pedrobtz.github.io/zuspark/reference/spark_interrupt.md)
  : Interrupt operations on the server

## About the package

- [`zuspark_info()`](https://pedrobtz.github.io/zuspark/reference/zuspark_info.md)
  : What zuspark was built with
- [`zuspark`](https://pedrobtz.github.io/zuspark/reference/zuspark-package.md)
  [`zuspark-package`](https://pedrobtz.github.io/zuspark/reference/zuspark-package.md)
  : zuspark: Native Client for 'Spark Connect'
