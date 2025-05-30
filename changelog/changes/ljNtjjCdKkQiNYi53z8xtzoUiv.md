---
title: "Fix predicate pushdown in `export` and other small fixes"
type: bugfix
authors: dominiklohmann
pr: 3599
---

A regression in Tenzir v4.3 caused exports to often consider all partitions as
candidates. Pipelines of the form `export | where <expr>` now work as expected
again and only load relevant partitions from disk.

The long option `--skip-empty` for `read lines` now works as documented.
