---
title: Parallel summarize with periodic emission
type: change
authors:
  - aljazerzen
created: 2026-08-21T15:12:52.468766Z
---

The `summarize` operator can run on multiple cores, even when `emit:
<duration>`. This means that outputs of groups might not be emitted at
aligned intervals.

For example:

```tql
// parallelism: 8
from ...
summarize key, count(), options={emit: 1h, mode: "reset"}
```

... given two input events:

```tql
{key: 'a'}  # at 7:00
{key: 'b'}  # at 7:20
```

... may now emit an aggregate for `a` at 8:00, and an aggregate for `b` at 8:20.
This depends on how parallel jobs are distributed, and can still result in
both aggregates being emitted at 8:00. When using `// parallelism: 1`, both
aggregates are guaranteed to emit at 8:00.

When it is important that the intervals are aligned, use `window` operator
instead.
