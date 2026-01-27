---
title: "Support comments in xsv parser"
type: feature
author: eliaskosunen
created: 2023-11-29T17:26:13Z
pr: 3681
---

Use `--allow-comments` with the `xsv` parser (incl. `csv`, `tsv`, and `ssv`)
to treat lines beginning with `'#'` as comments.
