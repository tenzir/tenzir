---
title: "Change `summarize` to operate across schemas"
type: change
authors: jachris
pr: 3250
---

The aggregation functions in a `summarize` operator can now receive only a
single extractor instead of multiple ones.

The behavior for absent columns and aggregations across multiple schemas was
changed.
