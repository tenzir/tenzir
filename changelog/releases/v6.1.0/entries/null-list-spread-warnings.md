---
title: Null list spread warnings
type: change
authors:
  - mavam
  - codex
created: 2026-06-06T04:37:42.295012Z
---

Spreading `null` into a list no longer emits a warning. This makes optional list-building expressions work without an explicit `else []` fallback:

```tql
from {xs: null}
items = [1, ...xs, 2]
```

The expression produces `[1, 2]`, and `concatenate`, `append`, and `prepend` follow the same warning-free behavior for `null` list inputs.
