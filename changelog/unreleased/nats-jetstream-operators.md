---
title: NATS JetStream operators
type: feature
authors:
  - mavam
  - codex
created: 2026-04-17T04:39:41.372977Z
---

Tenzir can now consume from and publish to NATS JetStream subjects with the
new `from_nats` and `to_nats` operators:

```tql
from_nats "alerts", metadata_field=nats
this = string(message).parse_json()
```

```tql
load "events.json"
to_nats "alerts", message=message
```

Both operators support configurable connection settings, authentication, and
the standard `tls` record. `from_nats` emits one event per message with the raw
payload in the `message` blob field and can attach NATS metadata with
`metadata_field`.
