---
title: Filters move through assignments and drop
type: change
authors:
  - mavam
created: 2026-09-30T11:11:51.455918Z
---

The optimizer now moves `where` filters in front of `set` and `drop` by rewriting them over the operator input. For example, `y = x | where y == 42` becomes `where x == 42 | y = x`. This also covers moved fields, constants, record literals, spread, and whole-event assignments such as `this = {source: this, ocsf: {}}` and `this = move this.ocsf`. A field that `move` or `drop` removes reads as `null`.

As a result, a filter on OCSF fields after a normalization operator now reaches the source, which can then fetch only the matching events. Filters that depend on nondeterministic values, such as `now()`, stay where they are.
