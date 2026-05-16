---
title: Prometheus shape for `metrics`
type: feature
authors:
  - mavam
  - codex
created: 2026-05-16T00:00:00Z
---

The `metrics` operator now accepts `shape="prometheus"` to emit metrics as
canonical `{metric, value, timestamp, labels, type, unit}` records. The default
remains `shape="raw"`, which preserves the existing `tenzir.metrics.*` schemas.
