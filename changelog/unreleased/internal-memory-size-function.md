---
title: Internal memory size function
type: feature
authors:
  - IyeOnline
  - codex
created: 2026-05-08T21:12:04.15977Z
---

The new `internal_memory_size` function estimates the size of each event in
bytes:

```tql
size = internal_memory_size(this)
```

This is useful for building pipelines that inspect or route events based on their approximate in-memory payload size.
