---
title: "More parsing functions"
type: feature
author: IyeOnline
created: 2025-02-05T21:21:10Z
pr: 4933
---

It is now possible to define additional patterns in the `parse_grok` function.

The `read_xsv` family of parsers now accept the `header` as a list of strings as
an alternative to a single delimited string.

`read_grok` now accepts additional `pattern_definitions` as either a `record`
mapping from pattern name to definition or a `string` of newline separated
patterns definitions.

We introduced the `parse_csv`, `parse_kv`, `parse_ssv`, `parse_tsv`, `parse_xsv` and
`parse_yaml` functions, allowing you to parse strings as those formats.

The `map` function now handles cases where list elements mapped to different types.
