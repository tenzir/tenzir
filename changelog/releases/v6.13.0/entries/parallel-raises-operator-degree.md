---
title: Rework `parallel` operator
type: change
authors:
  - aljazerzen
created: 2026-08-14T00:00:00.000000Z
---

A `parallel` block is now semantically transparent. Operators inside it observe
the same events, in the same shape, as they would without it, so you no longer
have to reason about how many instances of an operator exist or about a
boundary between an outer and an inner pipeline.

The `route_by` option has been deprecated and is now automatically inferred.

`parallel N { … }` raises the degree only for parallelizable operators.
Operators that cannot run as multiple instances continue to run as one.
Sources are among them, so `parallel N { … }` around a source no longer
produces `N` copies of every event.
