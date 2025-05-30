---
title: "Allow aggregation functions to be called on lists"
type: feature
authors: dominiklohmann
pr: 4821
---

Aggregation functions now work on lists. For example, `[1, 2, 3].sum()` will
return `6`, and `["foo", "bar", "baz"].map(x, x == "bar").any()` will return
`true`.
