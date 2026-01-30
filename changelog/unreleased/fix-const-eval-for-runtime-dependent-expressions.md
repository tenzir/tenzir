---
title: Fix const-eval for runtime-dependent expressions
type: bugfix
authors:
  - jachris
pr: 5701
created: 2026-01-30T12:47:26.000000Z
---

Expressions containing the `in` operator combined with `map` and lambdas that
reference runtime fields are no longer incorrectly const-evaluated.
