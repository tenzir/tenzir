---
title: "Gracefully handle null values when charting with resolution"
type: bugfix
authors: raxyte
pr: 5273
---

The `chart_bar` and `chart_pie` operators had a bug when the x-axis had a
`null` value and the `resolution` option was specified. The unfortunate panic
due to this bug has now been fixed.
