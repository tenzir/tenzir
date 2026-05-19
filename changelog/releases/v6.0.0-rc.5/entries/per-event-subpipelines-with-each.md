---
title: Per-event subpipelines with `each`
type: feature
author: jachris
pr: 5981
created: 2026-04-30T12:56:25.237501Z
---

The new `each` operator runs a fresh subpipeline for every input event. The
event is bound to `$this` inside the subpipeline so it can parametrize the
nested logic on a per-event basis:

```tql
from [
  {file: "a.json"},
  {file: "b.json"},
]
each {
  from $this.file
}
```

The subpipeline takes no input from `each`. It either emits events—which are
forwarded as the operator's output—and may also end with a sink, in which case
`each` itself becomes a sink.

Use `each` for per-event jobs such as a lookup, an export, or a sink whose
source depends on the incoming event. For keyed streams that should keep one
subpipeline alive per key, use `group` instead.
