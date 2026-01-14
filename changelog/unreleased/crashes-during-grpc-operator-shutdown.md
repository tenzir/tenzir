---
title: Crashes during gRPC operator shutdown
type: bugfix
authors:
  - mavam
  - claude
pr: 5661
created: 2026-01-14T16:26:58.265149Z
---

We fixed critical concurrency bugs in the `from_google_cloud_pubsub`, `to_google_cloud_pubsub`, and `from_velociraptor` operators that could cause crashes.

The `from_google_cloud_pubsub` operator had two issues: a data race between the subscription callback and the main loop accessing the series builder concurrently, and missing synchronization when cancelling the subscription. The `to_google_cloud_pubsub` operator didn't flush pending publishes before destruction. The `from_velociraptor` operator didn't properly shut down its gRPC completion queue.

These fixes eliminate SIGSEGV crashes (signal 11), assertion failures, and heap corruption that could occur during high-throughput gRPC operations or pipeline shutdown.
