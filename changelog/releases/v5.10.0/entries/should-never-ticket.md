---
title: "Fixed `list_separator` option name in `print_csv`"
type: bugfix
author: IyeOnline
created: 2025-07-22T07:43:32Z
pr: 5357
---

The `print_csv`, `print_ssv` and `print_tsv` functions had an option incorrectly
named `field_separator`. Instead, these functions have an option `list_separator`
now, allowing you to change the list separator.
You cannot set a custom `field_separator` on these functions. If you want to
print with custom `field_separator`s, use `print_xsv` instead.
