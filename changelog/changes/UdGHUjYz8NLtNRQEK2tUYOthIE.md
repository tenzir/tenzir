---
title: "Make the expression evaluator support heterogeneous results"
type: change
authors: jachris
pr: 4839
---

Functions can now return values of different types for the same input types. For
example, `x.otherwise(y)` no longer requires that `x` has the same type as `y`.
