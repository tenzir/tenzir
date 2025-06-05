---
title: "Check for duplicate field names in zeek_tsv_parser"
type: bugfix
authors: eliaskosunen
pr: 3578
---

Having duplicate field names in `zeek-tsv` data no longer causes a crash,
but rather errors out gracefully.
