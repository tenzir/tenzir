---
title: "Fixed excessive memory allocation"
type: bugfix
author: [IyeOnline,jachris,raxyte]
created: 2025-11-12T15:01:02Z
pr: 5561
---

We have switched the allocator back to the system allocator for now, as the
`mimalloc` setup led to significant memory usage issues on some systems.

Furthermore, we added quite a few experimental memory-usage related metrics to
`metrics "memory"`, providing additional insight into the memory usage of
Tenzir.
