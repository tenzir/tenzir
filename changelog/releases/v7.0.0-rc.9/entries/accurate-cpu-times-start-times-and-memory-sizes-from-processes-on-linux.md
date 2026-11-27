---
title: Accurate CPU times, start times, and memory sizes from processes on Linux
type: bugfix
authors:
  - mavam
created: 2026-11-17T10:31:14.520741Z
---

On Linux, the `processes` operator reported `utime` and `stime` in whole seconds, which showed a mostly idle process as using no CPU at all, and left `startup` empty. It also reported `peak_mem` and `swap` in kilobytes rather than bytes, unlike `rsize` and `vsize`. The operator now reports CPU times to the millisecond, the time each process started, and all memory sizes in bytes.
