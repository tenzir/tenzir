---
title: Zero-offset timezone names in timestamp parsing
type: feature
authors:
  - zedoraps
  - claude
prs:
  - 6464
created: 2026-07-21T14:42:56.24664Z
---

The `time()` function and all other places that parse timestamps now accept
the zero-offset timezone names `GMT`, `UTC`, and `UT` as suffixes, optionally
preceded by a space:

```tql
from {x: "2026-07-21 13:55:59.000 GMT"}
x = x.time()
```

Previously, such timestamps failed to parse because only `Z` and numeric
offsets like `+02:00` were supported.

Additionally, `time()` now emits a warning when it fails to parse a string
instead of silently returning `null`.
