---
title: Crash when passing a non-lambda to map or where
type: bugfix
authors:
  - jachris
prs:
  - 6256
created: 2026-06-05T15:32:00.144565Z
---

Calling the `map` or `where` list functions with a non-lambda argument now
fails with a clear `expected a lambda` diagnostic instead of aborting the
pipeline with an internal error.

For example, the following pipeline previously crashed and now reports a
proper error:

```tql
from {xs: [1, 2, 3]}
xs = xs.map(null)
```
