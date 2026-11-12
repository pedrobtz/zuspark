p <- processx::process$new("Rscript", "p08_child.R", stdout = "out_p08.txt", stderr = "2>&1")
Sys.sleep(135); p$signal(tools::SIGINT); p$wait(40000)
if (p$is_alive()) { cat("STILL ALIVE, killing\n"); p$kill() }
cat("exit", p$get_exit_status(), "\n"); writeLines(readLines("out_p08.txt"))
