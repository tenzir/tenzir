---
title: "Change `summarize` to operate across schemas"
type: change
author: jachris
created: 2023-06-23T15:57:13Z
pr: 3250
---

The aggregation functions in a `summarize` operator can now receive only a
single extractor instead of multiple ones.

The behavior for absent columns and aggregations across multiple schemas was
changed.
