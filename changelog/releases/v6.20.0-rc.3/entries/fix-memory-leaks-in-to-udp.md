---
title: Fix memory leaks in to_udp
type: bugfix
authors:
  - IyeOnline
created: 2026-10-07T11:00:00.12845Z
---

Fixed memory leaks in `to_udp` caused by incorrect cleanup of coroutine structured bindings in optimized GCC builds. Nix builds now use GCC 16, which correctly releases these allocations.
