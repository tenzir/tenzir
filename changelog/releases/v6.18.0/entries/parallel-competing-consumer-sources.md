---
title: Parallel competing-consumer sources
type: change
authors:
  - aljazerzen
created: 2026-09-14T10:44:44.966109Z
---

Pipelines configured with parallelism can now run multiple `from_google_cloud_pubsub` and `from_amqp` consumers when their broker configuration safely distributes messages between them. AMQP configurations that create independent consumers or reject additional consumers remain single-instance.
