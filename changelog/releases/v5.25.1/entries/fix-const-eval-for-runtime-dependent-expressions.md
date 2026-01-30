---
title: Fix overzealous constant evaluation in `if` statements
type: bugfix
authors:
  - jachris
pr: 5701
created: 2026-01-30T12:47:26.000000Z
---

The condition of `if` statements is no longer erroneously evaluated early when
it contains a lambda expression that references runtime fields.
