---
title: "Improve usability of CSV format"
type: feature
author: dominiklohmann
created: 2022-06-10T14:31:29Z
pr: 2336
---

The `csv` import gained a new `--seperator='x'` option that defaults to `','`. Set
it to `'\t'` to import tab-separated values, or `' '` to import space-separated
values.
