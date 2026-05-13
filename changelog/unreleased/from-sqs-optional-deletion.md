---
title: SQS receive controls
type: feature
authors:
  - mavam
  - codex
pr: 6167
created: 2026-05-13T00:00:00Z
---

The `from_sqs` operator now gives you explicit control over how messages are
received from SQS. Use `delete=false` to inspect or replay messages without
removing them from the queue, `batch_size=<1..10>` to control how many messages
each receive request may return, and `visibility_timeout=<duration>` to override
the queue visibility timeout for received messages:

```tql
from_sqs "events", delete=false, batch_size=10, visibility_timeout=30s
```

By default, `from_sqs` keeps deleting each received message after emitting it.
With `delete=false`, SQS makes the message visible again after the queue's
visibility timeout.
