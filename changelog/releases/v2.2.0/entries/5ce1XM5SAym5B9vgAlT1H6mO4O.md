---
title: "Summarize operator with pluggable aggregation functions"
type: feature
author: dominiklohmann
created: 2022-07-11T16:11:58Z
pr: 2417
---

The `summarize` operator supports three new aggregation functions: `sample`
takes the first value in every group, `distinct` filters out duplicate values,
and `count` yields the number of values.
