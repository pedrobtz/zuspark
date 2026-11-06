# PySpark's side of tools/run-benchmarks.R (design §10): the same
# operations against the same Spark Connect server, timed the same way.
# Needs pyspark-client 4.2.x and pyarrow:
#
#   python3 -m venv /tmp/pyspark && /tmp/pyspark/bin/pip install \
#     "pyspark-client==4.2.1" pyarrow pandas
#   /tmp/pyspark/bin/python tools/benchmarks.py sc://localhost:15002
#
# Prints one JSON object of median seconds.

import json
import statistics
import sys
import time

from pyspark.sql import SparkSession

remote = sys.argv[1] if len(sys.argv) > 1 else "sc://localhost:15002"
reps = {"connect": 5, "select1": 50, "collect": 3}


def median_time(f, n):
    times = []
    for _ in range(n):
        t = time.perf_counter()
        f()
        times.append(time.perf_counter() - t)
    return statistics.median(times)


def connect():
    s = SparkSession.builder.remote(remote).create()
    s.version
    s.stop()


out = {"pyspark": __import__("pyspark").__version__}
connect()
out["connect"] = median_time(connect, reps["connect"])

spark = SparkSession.builder.remote(remote).create()
spark.sql("SELECT 1").collect()
out["select1"] = median_time(lambda: spark.sql("SELECT 1").collect(), reps["select1"])

wide = "SELECT id, id * 2 AS a, CAST(id AS DOUBLE) AS b, id % 7 AS c FROM range(10000000)"
spark.sql(wide).toArrow()
out["collect"] = median_time(lambda: spark.sql(wide).toArrow(), reps["collect"])
out["collect_pandas"] = median_time(lambda: spark.sql(wide).toPandas(), reps["collect"])
spark.stop()

print(json.dumps(out))
