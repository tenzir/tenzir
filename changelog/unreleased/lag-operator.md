---
title: Lag operator
type: feature
authors:
  - mavam
prs:
  - 194
created: 2026-08-24T08:48:14.302022Z
---

Many detections need to compare an event with the preceding event. The new
`lag` operator adds that earlier value or complete event to the current event
while preserving the input stream. Compose it with `group` to maintain an
independent history for each user, host, process, or session:

```tql
from \
  {sequence: 1, user: "alice", location: "Berlin"},
  {sequence: 2, user: "bob", location: "Paris"},
  {sequence: 3, user: "alice", location: "London"}
group user {
  lag value=location, into=previous_location
}
sort sequence
drop sequence
```

```tql
{user: "alice", location: "Berlin", previous_location: null}
{user: "bob", location: "Paris", previous_location: null}
{user: "alice", location: "London", previous_location: "Berlin"}
```

The third event now contains Alice's last observed location, while Bob's
history remains independent. The final sort restores the original order for
this bounded example; `group` preserves order within each user but not between
users. A login detection can attach the preceding location and timestamp for
each user, calculate the distance and elapsed time, and flag impossible travel
or possible account sharing. The same pattern can detect state transitions for
accounts, processes, and sessions or calculate deltas between adjacent metrics.

Set `offset` to select an earlier event. Omit `value` to attach the complete
preceding event. Put `reorder` before `lag` when event-time input can arrive out
of order. Without an enclosing window, `group` retains one subpipeline and
`lag` retains `offset` values per key until the input ends. Place grouped lag
operations inside `window` when its boundaries fit the detection semantics.
Unlike a trailing count window, `lag` doesn't replay retained events through a
subpipeline for every input event.
