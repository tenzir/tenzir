---
title: Graceful S3 close failures in legacy pipelines
type: bugfix
authors:
  - jachris
  - codex
prs:
  - 6457
created: 2026-07-19T08:55:36.358905Z
---

Pipelines running with `neo: false` no longer abort the node when closing an
S3 output stream fails, including S3-backed `to_hive` pipelines. The close
failure now stops only the affected pipeline with an error.
