---
title: Conjunctions split for filter pushdown
type: change
authors:
  - mavam
created: 2026-10-15T11:18:06.000000Z
---

Operators that move filters upstream now move each operand of a top-level `and` on its own. Previously, a `where` with a single operand that could not move past an operator stayed entirely behind it. For example, when a mapping assigns a time field with a nondeterministic fallback, a source now still receives the field filter:

```tql
from_duckdb "events.duckdb", table="network"
time = time else now()
where src_endpoint.ip == 203.0.113.7 and time > 2026-01-01
```

Here, `src_endpoint.ip == 203.0.113.7` reaches the source, while `time > 2026-01-01` stays behind the assignment. A `where a and b` keeps the same events as `where a | where b`, including when an operand is `null`, so results do not change. Only the top-level `and` splits, not one inside `or` or `not`.
