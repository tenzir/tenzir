---
title: "Implement some `print_*` functions"
type: feature
author: IyeOnline
created: 2025-02-25T12:45:41Z
pr: 5001
---

The `write_xsv` family of operators now accepts multi-character separators,
instead of being restricted to a single character.

We added the `write_kv` operator, allowing you to write events as Key-Value
pairs.

We added the functions `any.print_json()` and `any.print_yaml()` to print any
value as a JSON or YAML string.

We added the functions `record.print_kv()`, `record.print_csv()`,
`record.print_ssv()`, `record.print_tsv()` and `record.print_xsv()` to print
records as the respective format.
