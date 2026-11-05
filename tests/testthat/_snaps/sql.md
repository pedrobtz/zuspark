# the encoded plans are stable

    Code
      cat(paste(format(as.hexmode(as.integer(bytes)), width = 2), collapse = ""),
      "\n")
    Output
      12382a360a1753454c4543542069642046524f4d2072616e6765283529321b52190a1753454c4543542069642046524f4d2072616e6765283529 
    Code
      cat(zuproto::proto_to_json(bytes, "spark.connect.Plan", spark_pool()), "\n")
    Output
      {"command":{"sqlCommand":{"sql":"SELECT id FROM range(5)","input":{"sql":{"query":"SELECT id FROM range(5)"}}}}} 

---

    Code
      cat(paste(format(as.hexmode(as.integer(bytes)), width = 2), collapse = ""),
      "\n")
    Output
      0a4552430a4153454c4543542069642c206964202a20322041532074776963652c204341535428696420415320535452494e472920415320732046524f4d2072616e6765283529 
    Code
      cat(zuproto::proto_to_json(bytes, "spark.connect.Plan", spark_pool()), "\n")
    Output
      {"root":{"sql":{"query":"SELECT id, id * 2 AS twice, CAST(id AS STRING) AS s FROM range(5)"}}} 

