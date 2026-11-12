# Target map, zuspark 17f825c

| Target | Rows | Suite weakness |
| --- | --- | --- |
| Boundaries, emitted | Spark Connect requests (Config, AnalyzePlan, ExecutePlan, ReattachExecute, ReleaseExecute, Interrupt, ReleaseSession, FetchErrorDetails); request metadata; Spark SQL text from DBI (quoting, literals, VALUES select) | requests compared only with zuspark's own recording (#107) |
| Boundaries, decoded | ExecutePlan stream; unary answers; ErrorInfo metadata JSON (hand parser); FetchErrorDetails; DataType to DDL text; Arrow batches via zuarrow | JSON (#109), DDL (#108) hand-written oracles |
| Text in | sc:// connection string; Databricks host; headers; settings | hand-worked from PySpark (#110) |
| Limits | release_every 16; 3 stalled reattaches; user_agent 2048 encoded chars; Spark >= 3.5.0; port 0-65535; deadline > 0 (default 30, finaliser 5); grpc_max_message_size and keepalive (to zugrpc); write_warn_rows 10000; print 70 chars; batch buffer 16, doubled | almost none straddled (#115-#122) |
| Objects | zuspark_connection (environment, channels registry, finaliser, server session); ExecutePlan operation; zuspark_query; spark_fetch() stream; DBI result/connection; descriptor pool | fork (#112), finaliser (#113), fetch stream (#114), DBItest not run (#111) |
| Numbers into C | none in zuspark; numbers go to zugrpc (max_message, keepalive, deadline) and zuarrow | |
| Second cases | repeated URL parameter keys; repeated header names; userinfo; IPv6; query/fragment; response_id empty; operation_id empty; schema not a struct; observed metrics accumulating across reattach | |
| Platform branches | none in R/ beyond os/ in client_type; the TLS path (Databricks) runs only through a recorded fixture; Spark 3.5 servers not available | |
