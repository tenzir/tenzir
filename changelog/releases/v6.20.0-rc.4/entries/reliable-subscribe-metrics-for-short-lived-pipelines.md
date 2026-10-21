---
title: Reliable subscribe metrics for short-lived pipelines
type: bugfix
authors:
  - aljazerzen
created: 2026-10-09T00:00:00.000000Z
---

The `subscribe` operator no longer loses its `tenzir.metrics.subscribe` and
`tenzir.metrics.subscribe_buffer` events for pipelines that finish within the
one-second metrics interval. Previously, such pipelines only emitted their
metrics during an asynchronous teardown that could outlive the pipeline's
metrics receiver, in which case the metrics were silently dropped.
