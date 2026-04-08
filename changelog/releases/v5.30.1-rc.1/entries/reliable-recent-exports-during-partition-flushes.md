---
title: Reliable recent exports during partition flushes
type: bugfix
authors:
  - tobim
  - codex
created: 2026-03-30T10:34:28.58853Z
---

The `export` command no longer fails or misses recent events when a node is flushing active partitions to disk under heavy load. Recent exports now keep the in-memory partitions they depend on alive until the snapshot completes, which preserves correctness for concurrent import and export workloads.
