---
title: "Implement some `print_*` functions"
type: change
author: IyeOnline
created: 2025-02-25T12:45:41Z
pr: 5001
---

The `sep` argument on the `flatten` and `unflatten` functions is now a
positional argument, allowing you to simply write `record.flatten("-")`.

The `unflatten` option found on many `read_*` operators and `parse_*` functions
is now called `unflatten_separator`.

The `field_sep`, `list_sep` and `null_value` options on the XSV operators and
functions (such as `read_xsv`, `write_csv` or `parse_tsv`) are now named
arguments on all of them and are called `field_separator`, `list_separator`
and `null_value`.

The `field_split` and `list_split` arguments for the `read_kv` operator and
`parse_kv` function are now named arguments.
