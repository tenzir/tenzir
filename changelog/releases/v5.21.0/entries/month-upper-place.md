---
title: "Easier multi-key deduplication"
type: change
author: mavam
created: 2025-11-14T17:59:49Z
pr: 5570
---

We now support multiple keys for deduplication, e.g., `deduplicate a, b.c, d`
is now equivalent to the previously unwieldy expression `deduplicate {x: a, y:
b.c, z: d}`.
