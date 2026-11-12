p <- processx::process$new("Rscript", "p17_child.R", stdout = "out17.txt", stderr = "2>&1")
Sys.sleep(6); system("podman pause hunt3-spark"); Sys.sleep(2); t0 <- Sys.time(); p$signal(tools::SIGINT); p$wait(100000)
cat("child ended", round(as.numeric(Sys.time() - t0, units = "secs"), 1), "s after SIGINT; alive:", p$is_alive(), "\n"); if (p$is_alive()) p$kill()
system("podman unpause hunt3-spark"); writeLines(readLines("out17.txt"))
