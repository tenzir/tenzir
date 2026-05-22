---
title: Reduce disk I/O of time-based compaction
type: bugfix
authors:
  - lava
created: 2026-05-12T13:32:50.629468Z
---

Time-based compaction rules no longer cause the node to reprocess data that
has already been compacted in a previous run.
