---
title: NATS JetStream operators
type: feature
authors:
  - mavam
  - codex
created: 2026-04-17T04:39:41.372977Z
---

Tenzir can now consume from and publish to NATS JetStream subjects with
`from_nats` and `to_nats`.

Use `from_nats` to receive one event per message. The raw payload appears in the
`message` blob field, and `metadata_field` attaches NATS metadata:

```tql
from_nats "alerts", metadata_field=nats
parsed = string(message).parse_json()
```

Use `to_nats` to publish one message per event. By default, the operator
serializes the whole event with `this.print_ndjson()`:

```tql
from {severity: "high", alert_type: "suspicious-login"}
to_nats "alerts"
```

Both operators support configurable connection settings, authentication, and
the standard Tenzir `tls` record.
