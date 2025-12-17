---
title: "Always enable time and bool synopses"
type: feature
author: dominiklohmann
created: 2023-11-15T10:10:12Z
pr: 3639
---

Lookups against uint64, int64, double, and duration fields now always use sparse
indexes, which improves the performance of `export | where <expression>` for
some expressions.
