source("/private/tmp/claude-501/-Users-pbtz-src-github-com-pedrobtz-zuspark/23786a96-505c-4b1e-b66f-e320fa1ada7b/scratchpad/hunt/hunt-2/harness.R")
p <- zuspark:::parse_json_strings
for (j in c("{}", "{ }", "[]", "{\"a\":\"b\"}", "[\"x\"]", "{\"a\":\"b\",\"a\":\"c\"}")) { cat(j, "->\n"); str(p(j)) }
