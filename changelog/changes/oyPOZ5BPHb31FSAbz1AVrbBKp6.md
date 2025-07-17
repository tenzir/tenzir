---
title: "Improve usability of CSV format"
type: feature
authors: dominiklohmann
pr: 2336
---

The `csv` import gained a new `--seperator='x'` option that defaults to `','`. Set
it to `'\t'` to import tab-separated values, or `' '` to import space-separated
values.
