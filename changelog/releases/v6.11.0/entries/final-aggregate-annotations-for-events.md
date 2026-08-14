---
title: Final aggregate annotations for events
type: feature
authors:
  - mavam
created: 2026-08-14T04:30:09.377919Z
---

The `summarize` operator can now attach final aggregate values to every event in a finite input. Set `output: "events"` to preserve the original evidence while adding the completed statistics for each group:

```tql
from {user: "alice", value: 10},
     {user: "alice", value: 20},
     {user: "bob", value: 5}
summarize user, avg=mean(value), samples=count(),
  options={output: "events"}
```

This output policy provides the TQL counterpart to Splunk's `eventstats` and composes with `window` for bounded populations. It is separate from `mode`, which controls whether aggregate state resets or accumulates across emission boundaries.
