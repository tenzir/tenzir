---
title: Force stop action for pipelines
type: feature
authors:
  - aljazerzen
  - claude
prs:
  - 6442
created: 2026-07-13T08:43:36.753785Z
---

Pipelines now support a `force-stop` action that terminates a pipeline
immediately instead of waiting for in-flight data to drain.

A regular `stop` moves a running pipeline into the `stopping` state and lets it
drain gracefully, which can take up to the configured
`tenzir.shutdown-grace-period`.

Sending `force-stop` to a pipeline that is already `stopping` cancels the grace
period and kills it immediately.
