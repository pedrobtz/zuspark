# zuspark: Native Client for 'Spark Connect'

Connects to a 'Spark Connect' endpoint, runs 'Spark SQL' statements
there and returns the results as 'Arrow' data or data frames, without a
'Java' virtual machine, 'Python' or a 'Spark' installation. Speaks the
'Spark Connect' protocol itself, in 'R': 'gRPC' calls through 'zugrpc',
protocol buffer messages through 'zuproto' and 'Arrow' record batches
through 'zuarrow'. Long results are read with reattachable execution, so
a stream cut by a proxy or a timeout resumes where it stopped. Works
with a local 'Spark' server, a cluster, and 'Databricks' workspaces,
including their serverless compute.

## See also

Useful links:

- <https://pedrobtz.github.io/zuspark/>

## Author

**Maintainer**: Pedro Baltazar <pedrobtz@gmail.com> \[copyright holder\]

Authors:

- Pedro Baltazar <pedrobtz@gmail.com> \[copyright holder\]

Other contributors:

- The Apache Software Foundation (Spark Connect protocol definitions in
  inst/proto) \[copyright holder\]

- Google LLC (google/rpc protocol definitions in inst/proto) \[copyright
  holder\]
