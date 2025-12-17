---
title: "Allow values to be `null` when charting"
type: change
author: raxyte
created: 2025-02-20T15:56:53Z
pr: 5009
---

The `chart_area`, `chart_bar`, and `chart_pie` operators no longer reject
null-values. Previously, gaps in charts were only supported for `chart_line`.
