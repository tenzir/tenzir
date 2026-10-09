---
title: "Memory leak in `to_udp`"
type: bugfix
authors:
  - tobim
created: 2026-10-08T11:59:15.162889Z
---

The `to_udp` operator no longer leaks memory for every batch of events it
sends. The leak affected builds compiled with GCC 15 and grew steadily in
long-running pipelines.
