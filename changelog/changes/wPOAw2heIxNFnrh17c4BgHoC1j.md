---
title: "Clean up transform steps (and native plugins generally)"
type: change
authors: dominiklohmann
pr: 2228
---

Multiple transform steps now have new names: `select` is now called `where`,
`delete` is now called `drop`, `project` is now called `put`, and `aggregate` is
now called `summarize`. This breaking change is in preparation for an upcoming
feature that improves the capability of VAST's query language.

The `layout-names` option of the `rename` transform step was renamed `schemas`.
The step now additonally supports renaming `fields`.
