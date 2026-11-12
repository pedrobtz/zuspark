source("pre.R")
con <- spark_connect(URL, session_id = "36436f25-33a8-4089-84e1-4ddb6e9433e6")
print(spark_interrupt(con)); print(spark_interrupt(con, "bf6a144b-dfa6-410f-b1bd-d341d8a05556"))
print(spark_disconnect(con))
