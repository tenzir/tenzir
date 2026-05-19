---
title: SQS receive controls
type: feature
authors:
  - mavam
  - codex
prs:
  - 6167
  - 6174
created: 2026-05-13T00:00:00Z
---

The `from_sqs` operator now gives you explicit control over how messages are
received from SQS. Use `keep_messages=true` to inspect or replay messages
without removing them from the queue, `batch_size=<1..10>` to control how many
messages each receive request may return, and `visibility_timeout=<duration>` to
override the queue visibility timeout for received messages:

```tql
from_sqs "events", keep_messages=true, batch_size=10, visibility_timeout=30s
```

By default, `from_sqs` keeps deleting each received message after emitting it.
With `keep_messages=true`, SQS makes the message visible again after the queue's
visibility timeout.
