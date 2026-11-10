---
title: Pipelines with `if` or `match` before `head` end
type: bugfix
authors:
  - zedoraps
created: 2026-11-07T16:00:00Z
---

A pipeline now ends once `head`, `slice`, or `taste` has what it needs, even if
an `if` or a `match` comes between them and the source. Before, the operators
upstream of the `if` or `match` were never told to stop, so a source that keeps
producing, such as `every` or `subscribe`, kept the
pipeline running after its last event:

```tql
every 1h {
  from {x: 1}, {x: 2}, {x: 3}
}
if x > 1 {
  y = 1
}
head 2
```

This pipeline printed two events and then never ended.
