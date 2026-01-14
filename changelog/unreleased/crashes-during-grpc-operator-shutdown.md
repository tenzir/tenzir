---
title: Crashes during gRPC operator shutdown
type: bugfix
authors:
  - mavam
  - claude
created: 2026-01-14T16:26:58.265149Z
---

We fixed critical use-after-free bugs in the `from_google_cloud_pubsub`, `to_google_cloud_pubsub`, and `from_velociraptor` operators that could cause crashes during shutdown.

The issues occurred when gRPC background threads continued accessing destroyed resources after operators completed. The `from_google_cloud_pubsub` operator now waits for the subscription session to fully stop before cleaning up. The `to_google_cloud_pubsub` operator now flushes all pending publishes before destruction. The `from_velociraptor` operator now properly shuts down its completion queue and drains remaining events.

These fixes eliminate SIGSEGV crashes (signal 11, NULL pointer dereference) that could occur during pipeline shutdown, particularly in production environments with high-throughput gRPC operations.
