---
title: Incremental summaries for event streams
type: change
authors:
  - mavam
created: 2026-08-11T10:50:41.924008Z
---

The `summarize` operator can now add running aggregates to every input event or emit aggregates periodically in processing time. For example, this SPL query uses `streamstats` to compute a running byte sum and event count per user:

```spl
... | streamstats sum(bytes) AS running_bytes count AS events BY user
```

You can express the same unbounded running aggregates in TQL with cumulative event emission:

```tql
from {user: "alice", bytes: 10},
     {user: "bob", bytes: 5},
     {user: "alice", bytes: 20}
summarize running_bytes=sum(bytes), events=count(), user,
  options={mode: "cumulative"}
```

Per-event emission is the default when you set `mode` without `emit`. Set
`emit` to a positive integer to emit after that many input events. For example,
`emit: 1` emits every event, while `emit: 100` emits every 100th event and the
final event of a partial interval.

For periodic output, set `emit` to a duration and choose whether each emission resets or retains aggregate state:

```tql
summarize count=count(), options={emit: 5s, mode: "reset"}
```

Timer intervals must be at least 10ms; smaller values for `emit` (and the
deprecated `frequency`) are rejected.

The `frequency` option remains available temporarily and emits a migration
warning. Update existing pipelines as follows:

| Previous options | Replacement |
| --- | --- |
| `options={frequency: 5s}` | `options={emit: 5s, mode: "reset"}` |
| `options={frequency: 5s, mode: "reset"}` | `options={emit: 5s, mode: "reset"}` |
| `options={frequency: 5s, mode: "cumulative"}` | `options={emit: 5s, mode: "cumulative"}` |

The `"update"` mode is no longer supported.
