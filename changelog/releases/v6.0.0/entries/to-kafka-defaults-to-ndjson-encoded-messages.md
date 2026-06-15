---
title: '`to_kafka` defaults to NDJSON-encoded messages'
type: breaking
author: lava
pr: 5742
created: 2026-04-30T13:02:38.652347Z
---

The default `message` expression of the `to_kafka` operator is now
`this.print_ndjson()` instead of `this.print_json()`. Kafka messages are
single-line records by default, so each event is now emitted as a single
NDJSON line:

```json
{"timestamp":"2024-03-15T10:30:00.000000","source_ip":"192.168.1.100","alert_type":"brute_force"}
```

instead of pretty-printed multi-line JSON.

To restore the previous behavior, pass `message=this.print_json()`
explicitly.
