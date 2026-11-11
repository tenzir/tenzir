---
title: Byte metrics no longer count filtered rows
type: bugfix
authors:
  - IyeOnline
created: 2026-11-10T00:00:00.000000Z
---

Operator byte metrics in the new executor now count only the events that an
upstream filter lets through. Previously, they included the bytes of events
that the filter had already dropped:

```tql
subscribe "events"
where severity == "high"
discard
```

Here, `discard` now reports the bytes of the high-severity events only.
