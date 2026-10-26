---
title: Reliable output from concurrent subpipelines
type: bugfix
authors:
  - aljazerzen
created: 2026-10-23T10:55:30.398881Z
---

Pipelines that emit results from multiple subpipelines no longer intermittently lose events, hang, or fail with an internal error. In particular, adding field assignments after `group` now preserves results from every group. The fix also applies to affected pipelines using `each`, `window`, and stream or ZeroMQ sources.
