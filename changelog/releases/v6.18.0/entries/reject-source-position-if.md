---
title: Reject source-position if statements
type: bugfix
authors:
  - aljazerzen
created: 2026-09-11T09:21:39.127711Z
---

An `if` statement at the beginning of a pipeline now reports an error instead of terminating with an internal assertion failure.
