---
title: Improved Clickhouse Usability
type: feature
authors:
  - IyeOnline
  - codex
  - mavam
  - raxyte
pr: 5897
created: 2026-03-11T22:21:22.602464Z
---

The `to_clickhouse` operator now supports dynamic table names via an expression
`table=...`, which must evaluate to a `string`. If the value is not a valid
table name, the events will be dropped with a warning.

With this change, the operator will also create a database if it does not exist.

The prime use-case for this are OCSF event streams:

```tql
subscribe "ocsf"
ocsf::cast encode_variants=true, null_fill=true
to_clickhouse table=f"ocsf.{class_name.replace(" ","_")}", ...
```
