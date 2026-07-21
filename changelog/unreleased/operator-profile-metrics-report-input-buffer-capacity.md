---
title: Reduced operator channel buffer size
type: change
authors:
  - aljazerzen
  - claude
created: 2026-07-21T06:48:18.71574Z
---

The buffer capacity between operators is now 32 MiB, reduced from 100 MiB. This bounds how much in-flight data a pipeline holds: a pipeline with 11 operators now buffers at most 352 MiB instead of up to 1.1 GiB. Smaller buffers reduce the time it takes to stop a pipeline gracefully.
