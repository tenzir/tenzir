---
title: Optional deletion for `from_sqs`
type: feature
authors:
  - mavam
  - codex
created: 2026-05-13T00:00:00Z
---

The `from_sqs` operator now accepts `delete=false` to receive messages without
deleting them from the SQS queue. It also accepts `batch_size=<1..10>` to
control the maximum number of messages per receive request and
`visibility_timeout=<duration>` to override the queue visibility timeout for
received messages:

```tql
from_sqs "events", delete=false, batch_size=10, visibility_timeout=30s
```

By default, `from_sqs` keeps deleting each received message after emitting it.
With `delete=false`, SQS makes the message visible again after the queue's
visibility timeout.
