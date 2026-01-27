---
title: "Check for duplicate field names in zeek_tsv_parser"
type: bugfix
author: eliaskosunen
created: 2023-10-18T09:28:06Z
pr: 3578
---

Having duplicate field names in `zeek-tsv` data no longer causes a crash,
but rather errors out gracefully.
