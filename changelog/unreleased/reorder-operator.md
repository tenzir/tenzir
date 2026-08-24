---
title: Bounded event-time reordering
type: feature
authors:
  - mavam
created: 2026-08-24T00:00:00.000000Z
---

The new `reorder` operator puts events into timestamp order while buffering a
bounded span of event time. For example, this pipeline receives the event at
two seconds before the event at one second:

```tql
from {id: "first", time: 0s.from_epoch()},
     {id: "third", time: 2s.from_epoch()},
     {id: "second", time: 1s.from_epoch()}
reorder on=time, tolerance=2s
```

It produces:

```tql
{id: "first", time: 1970-01-01T00:00:00Z}
{id: "second", time: 1970-01-01T00:00:01Z}
{id: "third", time: 1970-01-01T00:00:02Z}
```

The operator preserves arrival order for equal timestamps and flushes its
remaining ordered events when finite input ends. Events that arrive too late or
have an invalid timestamp are dropped with a warning. Place `reorder` inside
`group` when each key needs an independent event-time watermark.
