---
title: "Add an operator for dumping schemas"
type: feature
authors: dominiklohmann
pr: 4147
---

The chart operator now accepts the flags `--x-axis-type` and `--y-axis-type`
for `bar`, `line`, and `area` charts, with the possible values being `log` and
`linear`, with `linear` as the default value. Setting these flags defines the
scale (logarithmic or linear) on the Tenzir App chart visualization.
