---
title: "Send data to Kafka topics with `to_kafka`"
type: feature
authors: raxyte
pr: 5460
---

The new `to_kafka` operator allows you to send one Kafka message per event,
making it easier to integrate Tenzir with tools that rely on the 1:1 correlation
between messages and events.

**Examples**

Use `to_kafka` to send JSON events to a topic:

```tql
subscribe "logs"
to_kafka "events", message=this.print_json()
```

Send specific field values with custom keys for partitioning:

```tql
subscribe "alerts"
to_kafka "metrics", message=alert_msg, key="server-01"
```
