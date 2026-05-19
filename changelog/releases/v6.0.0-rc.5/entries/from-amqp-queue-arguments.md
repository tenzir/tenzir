---
title: from_amqp queue arguments
type: feature
authors:
  - mavam
  - codex
prs:
  - 6139
created: 2026-05-08T09:13:46.830064Z
---

`from_amqp` now accepts a `queue_arguments` record for RabbitMQ queue declaration arguments:

```tql
from_amqp "amqp://broker/vhost",
          queue="events",
          queue_arguments={
            "x-queue-type": "quorum",
            "x-quorum-initial-group-size": 3
          }
```

Use this to declare queues with broker-specific settings such as quorum queues, maximum lengths, message TTLs, single active consumers, and dead-letter exchanges.
