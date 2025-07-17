---
title: "Change `summarize` to operate across schemas"
type: feature
authors: jachris
pr: 3250
---

The `summarize` operator now works across multiple schemas and can combine
events of different schemas into one group. It now also treats missing columns
as having `null` values.

The `by` clause of `summarize` is now optional. If it is omitted, all events
are assigned to the same group.
