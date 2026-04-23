---
title: Crash fix for deep left-associated where expressions
type: bugfix
authors:
  - tobim
  - codex
pr: 6068
created: 2026-04-23T10:04:04.944386Z
---

Tenzir no longer crashes on very deep left-associated boolean expressions in `where` clauses.

For example, pipelines with long generated predicates such as the following now evaluate normally instead of segfaulting:

```tql
from {}
where false or false or false or false
```

This is especially useful for generated detection rules and other pipelines that mechanically expand large `or` chains.
