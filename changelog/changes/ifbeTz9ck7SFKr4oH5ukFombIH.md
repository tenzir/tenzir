---
title: "PRs 1473-1485"
type: bugfix
authors: mavam
pr: 1473
---

A race condition during server shutdown could lead to an invariant violation,
resulting in a firing assertion. Streamlining the shutdown logic resolved the
issue.
