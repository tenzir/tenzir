---
title: "Always enable time and bool synopses"
type: feature
authors: dominiklohmann
pr: 3639
---

Lookups against uint64, int64, double, and duration fields now always use sparse
indexes, which improves the performance of `export | where <expression>` for
some expressions.
