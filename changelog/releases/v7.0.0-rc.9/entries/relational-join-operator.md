---
title: Relational join operator
type: feature
authors:
  - mavam
created: 2026-11-22T07:27:35.912155Z
---

The new `join` operator combines events from the pipeline input with events produced by a finite right-hand source subpipeline using a hash join.

Specify one or more positional key expressions (`key` for a shared expression, or `lhs == rhs` when the left and right expressions differ). Supported `kind` values are `"inner"` (default), `"left"`, `"semi"`, and `"anti"`. The right-hand source must finish and fit within `max_memory` (default `512 MiB`) before output begins. By default, `"inner"` and `"left"` joins emit `{left: ..., right: ...}` records; pass `into=<field>` to attach the matched right event onto the left event instead. Top-level `null` keys and any `NaN` keys (including nested `NaN`) never match.

```tql
from_file "alerts.json" { read_json }
join src_ip == ip, kind="left", into=asset {
  from_file "assets.json" { read_json }
}
```
