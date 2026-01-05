---
title: Per-pipeline memory consumption metrics
type: feature
author: jachris
pr: 5644
created: 2026-01-05T12:54:07.231597Z
---

The new `tenzir.metrics.operator_buffers` metrics track the total bytes and
events buffered across all execution nodes of a pipeline. The metrics are
emitted every second and include:

- `timestamp`: The point in time when the data was recorded
- `pipeline_id`: The pipeline's unique identifier
- `bytes`: Total bytes currently buffered
- `events`: Total events currently buffered (for events only)

Use `metrics "operator_buffers"` to access these metrics.
