---
title: Per-actor memory allocation tracking
type: feature
authors:
  - IyeOnline
pr: 5646
created: 2026-01-12T14:08:21.522154Z
---

We have added support for per-actor/per-thread allocation tracking. When enabled, these stats will track which actor
(or thread) allocated how much memory. This gives much more detailed insights into where memory is allocated.
By default these detailed statistics are not collected, as they introduce a cost to every allocation.
