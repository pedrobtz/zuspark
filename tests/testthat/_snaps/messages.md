# rpc and sql errors

    Code
      replay_connect("connect")
    Condition
      Error:
      ! UNAVAILABLE: connection refused

---

    Code
      spark_sql(con, "SELECT * FROM no_such_table")
    Condition
      Error:
      ! [TABLE_OR_VIEW_NOT_FOUND] The table or view `no_such_table` cannot be found.

# interrupt errors

    Code
      spark_collect(q)
    Condition
      Error:
      ! Interrupted: the server stopped no operation (it had finished).

# protocol errors

    Code
      spark_collect(q)
    Condition
      Error:
      ! A response for operation 00000000-0000-4000-8000-000000000000 arrived on operation 9fef00a2-cd37-44c3-b16a-165064c402c2.

# version errors

    Code
      replay_connect("connect")
    Condition
      Error:
      ! The server runs Spark 3.4.1; zuspark needs Spark 3.5.0 or later.

# url errors

    Code
      parse_spark_url("sc://h/;token=%zz")
    Condition
      Error:
      ! Parameter 'token' has an invalid percent-escape; write '%' as '%25'.
    Code
      parse_spark_url("sc://h:http")
    Condition
      Error:
      ! The port 'http' is not a number from 0 to 65535. Please update the URL to follow the correct format, e.g., 'sc://hostname:port'.
    Code
      spark_connect("sc://h", user_agent = 1)
    Condition
      Error:
      ! `user_agent` must be NULL or a single non-empty string.

# session errors

    Code
      spark_sql(con, "SELECT 1")
    Condition
      Error:
      ! This session was released by `spark_disconnect()`. Reconnect with `spark_connect()`.

