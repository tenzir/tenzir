---
title: "Fix a use-after-free in the `xsv` parser"
type: bugfix
author: dominiklohmann
created: 2024-09-04T13:15:21Z
pr: 4570
---

We fixed a potential crash in the `csv`, `ssv`, and `tsv` parsers for slowly
arriving inputs.
