---
title: Removal of the buffer operator
type: breaking
authors:
  - aljazerzen
created: 2026-09-24T15:16:22.303007Z
---

The disabled `buffer` operator has been removed. Remove `buffer` from existing pipelines, or use `batch` when you need to group events.

Before:

```tql
from {x: 1}
buffer 10
```

After:

```tql
from {x: 1}
batch 10
```
