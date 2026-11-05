# Writes PySpark's Arrow bytes for each row of design §7.7's type table:
# one IPC stream per query of tests/testthat/fixtures/types/types.csv, as
# PySpark's Spark Connect client receives it (DataFrame.toArrow()). The
# tests check that zuspark reads the same schema and values from the same
# server (test-live-types.R) and converts these bytes as zuarrow's rule
# says (test-types.R). Run inside the apache/spark:4.2.1 container, with
# pyarrow, pandas, grpcio, grpcio-status, googleapis-common-protos and
# zstandard installed:
#
#   PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
#     python3 pyspark-types.py types.csv out-dir
import csv
import os
import sys

import pyarrow as pa
from pyspark.sql import SparkSession

manifest, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
# Types PySpark itself cannot give as Arrow (toArrow() rebuilds the schema
# and refuses some) are listed, with PySpark's error class, in
# unsupported.csv; the tests check zuspark against the server alone there.
refused = []
with open(manifest, newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        try:
            table = spark.sql(row["sql"]).toArrow()
        except Exception as e:
            reason = getattr(e, "getErrorClass", lambda: None)() or type(e).__name__
            refused.append((row["name"], reason))
            print(row["name"], "refused:", reason)
            continue
        path = os.path.join(out, row["name"] + ".arrows")
        with pa.OSFile(path, "wb") as sink:
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
        print(row["name"], table.schema.field(0).type)

with open(os.path.join(out, "unsupported.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["name", "pyspark_error"])
    w.writerows(refused)
