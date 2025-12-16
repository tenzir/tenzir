---
title: Easier multi-key deduplication
type: change
authors: mavam
pr: 5570
---

We now support multiple keys for deduplication, e.g., `deduplicate a, b.c, d`
is now equivalent to the previously unwieldy expression `deduplicate {x: a, y:
b.c, z: d}`.
