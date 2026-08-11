---
title: Add a `merge` operator
type: feature
authors:
  - aljazerzen
---

The new `merge` operator merges the output of a source subpipeline into the main
event stream. It is the dual of `fork`: where `fork` attaches an additional sink
that consumes a copy of the input, `merge` attaches an additional source that
contributes events to the output.

```tql
subscribe "primary"
merge {
  subscribe "secondary"
}
publish "combined"
```
