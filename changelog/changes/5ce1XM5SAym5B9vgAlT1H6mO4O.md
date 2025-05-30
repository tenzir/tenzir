---
title: "Summarize operator with pluggable aggregation functions"
type: feature
authors: dominiklohmann
pr: 2417
---

The `summarize` operator supports three new aggregation functions: `sample`
takes the first value in every group, `distinct` filters out duplicate values,
and `count` yields the number of values.
