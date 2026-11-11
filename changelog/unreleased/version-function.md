---
title: The `version` function
type: feature
authors:
  - IyeOnline
created: 2026-11-11T00:00:00.000000Z
---

The new `version()` function returns the same record as the `version`
operator, so pipelines can use version information in expressions:

```tql
from {x: 1}
tenzir = version().version
```
