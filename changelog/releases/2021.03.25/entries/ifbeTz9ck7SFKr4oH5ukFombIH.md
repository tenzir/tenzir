---
title: "PRs 1473-1485"
type: bugfix
author: mavam
created: 2021-03-20T15:20:33Z
pr: 1473
---

A race condition during server shutdown could lead to an invariant violation,
resulting in a firing assertion. Streamlining the shutdown logic resolved the
issue.
