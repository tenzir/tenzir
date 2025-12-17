---
title: "Make the expression evaluator support heterogeneous results"
type: change
author: jachris
created: 2024-12-14T18:22:50Z
pr: 4839
---

Functions can now return values of different types for the same input types. For
example, `x.otherwise(y)` no longer requires that `x` has the same type as `y`.
