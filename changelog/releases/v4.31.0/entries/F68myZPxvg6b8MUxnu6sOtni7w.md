---
title: "Implement `if \u2026 else` expressions and short-circuiting for `and` / `or`"
type: feature
author: dominiklohmann
created: 2025-03-28T15:40:53Z
pr: 5085
---

Tenzir now supports inline `if … else` expressions in the form `foo if pred`,
which returns `foo` if `pred` evaluates to `true`, or `null` otherwise, and `foo
if pred else bar`, which instead of falling back to `null` falls back to `bar`.
