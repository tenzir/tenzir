---
title: "Fix bugs in `to_clickhouse` and improve diagnostics"
type: bugfix
authors: IyeOnline
pr: 5122
---

We fixed multiple bugs that caused unexpected internal failures and the creation
of potentially empty `Tuple`s in ClickHouse.
