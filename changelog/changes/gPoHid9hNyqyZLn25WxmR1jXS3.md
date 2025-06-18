---
title: "Implement `if … else` expressions and short-circuiting for `and` / `or`"
type: bugfix
authors: dominiklohmann
pr: 5085
---

The binary operators `and` and `or` now skip evaluating their right-hand side
when not necessary. For example, `where this.has("foo") and foo == 42` now
avoids emitting a warning when `foo` does not exist.
