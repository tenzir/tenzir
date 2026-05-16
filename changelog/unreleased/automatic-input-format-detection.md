---
title: Automatic input format detection
type: feature
authors:
  - mavam
  - codex
pr: 6191
created: 2026-05-16T14:52:28.856684Z
---

The new `read_auto` operator detects common input formats before choosing a reader:

```tql
from_stdin {
  read_auto
}
```

By default, `read_auto` fails when detection finds no unique match. Use `fallback="lines"` or `fallback="all"` to opt into generic line or whole-input reading for otherwise unknown input.
