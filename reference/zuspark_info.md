# What zuspark was built with

The versions a bug report needs: zuspark's, the Spark tag its protocol
definitions come from, and the gRPC and HTTP layers beneath it.

## Usage

``` r
zuspark_info()
```

## Value

A `zuspark_info` list: `version`, `spark` (the protocol's Spark tag),
`descriptors` (the SHA-256 of the shipped descriptor set), `zuproto` and
`zugrpc` (the latter's own `zugrpc_info()`).

## Examples

``` r
zuspark_info()
#> zuspark 0.1.0, Spark Connect protocol v4.2.1
#>   descriptors: fba77546a88f7e96
#>   zuproto:     0.1.0
#>   zugrpc:      0.0.0.9000 on zuhttp 0.1.0
```
