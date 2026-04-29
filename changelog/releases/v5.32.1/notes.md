This patch release fixes two correctness issues in stateful pipeline execution. Partition rebuilds now complete after writing replacement partitions, and periodic summarize output remains deterministic for delayed or sparse streams.

## 🐞 Bug fixes

### Deterministic periodic summarize output

The `summarize` operator now starts `frequency`-based emission with the first input event and emits overdue periodic results before later events are aggregated. This makes periodic output deterministic in `reset`, `cumulative`, and `update` modes for delayed or sparse streams.

For example:

```tql
from {ts: 0ms.from_epoch(), x: 1},
     {ts: 90ms.from_epoch(), x: 1},
     {ts: 360ms.from_epoch(), x: 1}
delay ts
summarize count=count(), options={frequency: 300ms, mode: "cumulative"}
```

The first periodic result now consistently reports a count of `2` before the third event arrives.

*By @mavam and @codex.*

### Partition rebuild completion

Partition rebuilds now finish after persisting rebuilt partitions. Previously, rebuild jobs could remain stuck indefinitely even though the replacement partitions were written successfully.

*By @tobim and @codex in #6059.*
