---
title: from_amqp queue arguments
type: feature
authors:
  - mavam
  - codex
pr: 6139
created: 2026-05-08T00:00:00Z
---

`from_amqp` now accepts a `queue_arguments` record for RabbitMQ queue declaration arguments. You can use it to declare queues with broker-specific settings such as quorum queues, maximum lengths, message TTLs, single active consumers, and dead-letter exchanges.
