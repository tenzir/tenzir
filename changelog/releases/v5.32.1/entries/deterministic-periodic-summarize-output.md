---
title: Deterministic periodic summarize output
type: bugfix
authors:
  - mavam
  - codex
created: 2026-04-16T15:25:20.087701Z
---

The `summarize` operator now starts `frequency`-based emission with the first
input event and emits overdue periodic results before later events are
aggregated. This makes periodic output deterministic in `reset`,
`cumulative`, and `update` modes for delayed or sparse streams.

For example:

```tql
from {ts: 0ms.from_epoch(), x: 1},
     {ts: 90ms.from_epoch(), x: 1},
     {ts: 360ms.from_epoch(), x: 1}
delay ts
summarize count=count(), options={frequency: 300ms, mode: "cumulative"}
```

The first periodic result now consistently reports a count of `2` before the
third event arrives.
