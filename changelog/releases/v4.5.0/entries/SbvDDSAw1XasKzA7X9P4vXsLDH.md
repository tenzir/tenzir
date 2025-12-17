---
title: "Improve `summarize` result for empty inputs"
type: feature
author: jachris
created: 2023-11-16T07:57:07Z
pr: 3640
---

If the `summarize` operator has no `by` clause, it now returns a result even
if there is no input. For example, `summarize num=count(.)` returns an event
with `{"num": 0}`. Aggregation functions which do not have a single default
value, for example because it would depend on the input type, return `null`.
