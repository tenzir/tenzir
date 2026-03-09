---
title: Graceful handling of Google Cloud Pub/Sub authentication errors
type: bugfix
authors:
  - mavam
  - codex
pr: 5877
created: 2026-03-09T09:01:12.640601Z
---

Invalid Google Cloud credentials in `from_google_cloud_pubsub` no longer crash the node. Authentication errors now surface as operator diagnostics instead.
