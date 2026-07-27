---
title: Warning for unparsable timestamps in time()
type: change
authors:
  - zedoraps
  - claude
prs:
  - 6464
created: 2026-07-22T12:15:41.378167Z
---

The `time()` function now warns when it cannot make sense of a string instead
of silently returning `null`. If your timestamps unexpectedly come out empty,
the warning tells you which value failed to parse:

```tql
from {x: "not a timestamp"}
x = x.time()
```

```
warning: `time` failed to parse string
  = note: tried to convert: not a timestamp
```
