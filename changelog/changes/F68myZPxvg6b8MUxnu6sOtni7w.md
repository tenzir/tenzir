---
title: "Implement `if … else` expressions and short-circuiting for `and` / `or`"
type: feature
authors: dominiklohmann
pr: 5085
---

Tenzir now supports inline `if … else` expressions in the form `foo if pred`,
which returns `foo` if `pred` evaluates to `true`, or `null` otherwise, and `foo
if pred else bar`, which instead of falling back to `null` falls back to `bar`.
