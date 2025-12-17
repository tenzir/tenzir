---
title: "Summarize operator with pluggable aggregation functions"
type: change
author: dominiklohmann
created: 2022-07-11T16:11:58Z
pr: 2417
---

The `summarize` pipeline operator is now a builtin; the previously bundled
`summarize` plugin no longer exists. Aggregation functions in the `summarize`
operator are now plugins, which makes them easily extensible. The syntax of
`summarize` now supports specification of output field names, similar to SQL's
`AS` in `SELECT f(x) AS name`.

The undocumented `count` pipeline operator no longer exists.
