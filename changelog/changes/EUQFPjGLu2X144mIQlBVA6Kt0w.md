---
title: "Fix a use-after-free in the `xsv` parser"
type: bugfix
authors: dominiklohmann
pr: 4570
---

We fixed a potential crash in the `csv`, `ssv`, and `tsv` parsers for slowly
arriving inputs.
