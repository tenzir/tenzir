---
title: Where operator optimization for optional fields
type: bugfix
authors:
  - jachris
  - claude
created: 2026-01-28T15:01:10.185807Z
---

The `where` operator optimization now correctly handles optional fields marked with `?`. Previously, the optimizer didn't account for the optional marker, which could result in incorrect query optimization. This fix ensures that optional field accesses are handled properly without affecting the optimization of regular field accesses.
