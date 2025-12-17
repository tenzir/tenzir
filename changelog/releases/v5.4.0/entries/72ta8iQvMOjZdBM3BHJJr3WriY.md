---
title: "Gracefully handle null values when charting with resolution"
type: bugfix
author: raxyte
created: 2025-06-11T14:59:06Z
pr: 5273
---

The `chart_bar` and `chart_pie` operators had a bug when the x-axis had a
`null` value and the `resolution` option was specified. The unfortunate panic
due to this bug has now been fixed.
