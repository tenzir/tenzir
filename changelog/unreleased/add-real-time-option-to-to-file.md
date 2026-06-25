---
title: Add `real_time` option to `to_file`
type: feature
authors:
  - jachris
created: 2026-06-25T15:26:00.082281Z
---

The `to_file` operator now buffers writes by default and coalesces them into
larger, less frequent operations for higher throughput. Set `real_time=true` to
write every batch straight through without buffering, trading throughput for
latency when another process tails the file. Buffered data is always flushed on
rotation, checkpoints, and close.
