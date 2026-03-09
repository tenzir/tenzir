---
title: Graceful handling of Google Cloud Pub/Sub authentication errors
type: bugfix
authors:
  - mavam
  - codex
pr: 5877
created: 2026-03-09T09:01:12.640601Z
---

Tenzir no longer aborts the node when `from_google_cloud_pubsub` encounters invalid Google Cloud authentication credentials. The operator now reports the subscriber error normally, so failed credentials surface as an operator error instead of a process crash. This makes authentication problems easier to diagnose without taking the whole node down.
