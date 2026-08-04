---
title: Lower resource usage for null-filling in ocsf::cast
type: change
authors:
  - tobim
created: 2026-07-30T17:49:59.005594Z
---

`ocsf::cast` with `null_fill=true` now uses significantly less memory and CPU. Previously, the operator rebuilt the null-filled columns for every batch, which roughly doubled peak memory usage for schema-complete casting of high-volume streams.
