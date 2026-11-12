source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/hunt-2/live.R")
ch <- zugrpc::grpc_channel("http://127.0.0.1:16999")
s <- tryCatch(zugrpc::grpc_stream(ch, "/spark.connect.SparkConnectService/ExecutePlan", raw(0)), error = function(e) e)
cat("grpc_stream() to a dead port:", if (inherits(s, "condition")) paste("RAISED AT OPEN:", paste(class(s), collapse = " ")) else "returned a stream", "\n")
if (!inherits(s, "condition")) { r <- tryCatch(s$next_message(), error = function(e) e); cat("next_message:", paste(class(r), collapse = " "), "\n") }
