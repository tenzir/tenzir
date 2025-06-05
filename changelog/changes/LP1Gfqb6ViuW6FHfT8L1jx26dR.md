---
title: "Allow values to be `null` when charting"
type: change
authors: raxyte
pr: 5009
---

The `chart_area`, `chart_bar`, and `chart_pie` operators no longer reject
null-values. Previously, gaps in charts were only supported for `chart_line`.
