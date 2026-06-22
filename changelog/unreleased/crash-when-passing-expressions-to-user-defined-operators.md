---
title: Crash when passing expressions to user-defined operators
type: bugfix
authors:
  - aljazerzen
  - claude
prs:
  - 6372
created: 2026-06-22T07:13:40.449968Z
---

Calling a user-defined operator with an expression argument no longer crashes
when the operator's body combines that argument with its own syntax. Previously,
passing an expression from one file into an operator defined in another could
abort the process while computing source locations for diagnostics.
