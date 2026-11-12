for (m in c("collect", "execute", "dbi")) {
  p <- processx::process$new("Rscript", c("p06_child.R", m), stdout = sprintf("out_%s.txt", m), stderr = "2>&1")
  Sys.sleep(7); p$signal(tools::SIGINT); p$wait(40000)
  if (p$is_alive()) { cat("STILL ALIVE, killing\n"); p$kill() }
  cat("== ", m, " exit ", p$get_exit_status(), "\n"); writeLines(readLines(sprintf("out_%s.txt", m)))
}
