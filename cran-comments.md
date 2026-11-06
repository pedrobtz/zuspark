## Status

zuspark 0.1.0 is a GitHub release. It is not submitted to CRAN yet: four
of its dependencies (zugrpc, zuproto, zuarrow, zuid) are not on CRAN, and
DESCRIPTION still names them in `Remotes:`. This file is kept for the
first CRAN submission.

## Test environments

Through the `pedrobtz/r-actions` workflows, full profile:

* macOS (latest), R release
* Windows (latest), R release and R-devel
* Ubuntu (latest), R release and R oldrel-1
* R-devel containers: clang 23, Ubuntu clang, Ubuntu gcc 16
* R-devel without suggested packages (`nosuggests`) and without long
  doubles (`nold`)
* Live tests against Spark Connect 4.2.1 in Docker (Ubuntu)

## R CMD check results

0 errors | 0 warnings | 0 notes

* This is a new release.
