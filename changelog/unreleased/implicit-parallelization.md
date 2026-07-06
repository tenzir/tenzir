---
title: Implicit parallelization of stateless pipeline regions
type: feature
authors:
  - aljazerzen
created: 2026-07-03T00:00:00.000000Z
---

Pipelines can now automatically parallelize regions of stateless operators
such as `where`, `set`, `select`, and `drop`. Set the new
`tenzir.implicit-parallelism` configuration option to the desired degree
(for example, 4) to enable it; the default of 1 keeps execution serial.

When enabled, the executor splits event streams row-wise across parallel
lanes at the start of a stateless region and merges them again at its end.
Stateful operators such as `sort`, `summarize`, and sources or sinks
continue to run as a single instance. Enabling implicit parallelism accepts
event reordering inside parallelized regions; use `sort` or an
order-insensitive aggregation downstream when order matters. Checkpointed
pipelines are excluded and keep running serially.
