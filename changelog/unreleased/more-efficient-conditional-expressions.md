---
title: More efficient conditional expressions
type: change
authors:
  - jachris
pr: 6246
created: 2026-06-24T13:16:43Z
---

Conditional expressions such as `x if y else z` are now significantly more
efficient than before when both branches share the same type or one side is
`null`. Downstream operators in particular benefit from this change, making
certain pipelines much faster as a result.
