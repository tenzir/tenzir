---
title: Crashes during gRPC operator shutdown
type: bugfix
authors:
  - mavam
  - claude
pr: 5661
created: 2026-01-14T16:26:58.265149Z
---

We fixed bugs in several gRPC-based operators:

- A potential crash in `from_velociraptor` on shutdown.
- Potentially not publishing final messages in `to_google_cloud_pubsub` on
  shutdown.
- A concurrency bug in `from_google_cloud_pubsub` that could cause a crash.
