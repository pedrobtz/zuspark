for (m in c("connect", "config", "schema", "disconnect", "interrupt", "collect")) {
  f <- sprintf("out16_%s.txt", m)
  p <- processx::process$new("Rscript", c("p16_child.R", m), stdout = f, stderr = "2>&1")
  Sys.sleep(4); p$signal(tools::SIGINT); p$wait(45000)
  if (p$is_alive()) { cat("STILL ALIVE, killing\n"); p$kill() }
  cat("== exit", p$get_exit_status(), "\n"); writeLines(readLines(f))
}
