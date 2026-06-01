---
title: More efficient conditional expressions
type: change
authors:
  - jachris
  - claude
pr: 6246
created: 2026-06-01T13:53:43Z
---

Conditional expressions such as `x if cond else y` (and the `a else b`
fallback) now produce fewer, larger result batches when both branches share a
type — or one side is `null` — instead of fragmenting the output wherever the
condition changes value. This lowers the per-batch overhead that downstream
operators pay when conditionals are evaluated over rapidly alternating
conditions. The resulting values and types are unchanged.
