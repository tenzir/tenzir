---
title: "Implement sub-duration functions"
type: feature
authors: raxyte
pr: 4985
---

New functions `years`, `months`, `weeks`, `days`, `hours`, `minutes`, `seconds`,
`milliseconds`, `microseconds` and `nanoseconds` convert a numeric value to the
equivalent duration. Their counterpart `count_*` functions calculate how many
units can the duration be broken into, i.e. `duration / unit`.

The `abs` function calculates the absolute value for a number or a duration.
