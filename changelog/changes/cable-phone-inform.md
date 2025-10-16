---
title: "Periodically release unused memory in server mode"
type: change
authors: lava
pr: 5524
---

Tenzir Node now calls malloc_trim every 10 minutes to release unused memory back to the operating system, reducing memory fragmentation in long-running instances.
