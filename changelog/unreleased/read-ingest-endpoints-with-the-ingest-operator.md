---
title: Read ingest endpoints with the ingest operator
type: feature
authors:
  - lava
created: 2026-10-28T12:29:35.03471Z
---

The new `ingest` operator reads the events sent to a Tenzir Platform ingest endpoint. Use `ingest "name"` in a pipeline on a deployment connected to the platform. Pass `group="…"` to share events between pipelines on different deployments.
