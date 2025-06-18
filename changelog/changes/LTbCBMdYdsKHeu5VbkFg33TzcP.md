---
title: "Add support for USDT tracepoints in VAST"
type: feature
authors: lava
pr: 1206
---

On Linux, VAST now contains a set of built-in USDT tracepoints that can be used
by tools like `perf` or `bpftrace` when debugging. Initially, we provide the two
tracepoints `chunk_make` and `chunk_destroy`, which trigger every time a
`vast::chunk` is created or destroyed.
