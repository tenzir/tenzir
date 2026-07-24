---
title: Zero-offset timezone names in timestamp parsing
type: change
authors:
  - zedoraps
  - claude
prs:
  - 6464
created: 2026-07-21T14:42:56.24664Z
---

Timestamps ending in `GMT`, `UTC`, or `UT`—common in web server and mail
logs—now parse out of the box, with or without a space before the name:

```tql
from {x: "2026-07-21 13:55:59.000 GMT"}
x = x.time()
```

Previously, such timestamps failed to parse because only `Z` and numeric
offsets like `+02:00` were supported.

Note that this also affects automatic type detection: values like
`"2026-07-21 13:55:59 GMT"` that previously stayed strings are now
recognized as timestamps, for example when reading CSV files.
