---
title: Fixed an assertion failure in slicing
type: bugfix
authors:
  - IyeOnline
pr: 5842
created: 2026-02-27T15:04:24.605924Z
---

We fixed a bug that would cause an assertion failure *"Index error: array slice would exceed array length"*.
This was introduced as part of an optimization in Tenzir Node v5.27.0.
