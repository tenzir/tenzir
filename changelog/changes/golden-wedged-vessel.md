---
title: "Getting Kafka records with `from_kafka`"
type: feature
authors: raxyte
pr: 5575
---

The new `from_kafka` operator allows you to receive one event per Kafka message,
thus keeping the event boundary unlike `load_kafka`, which has now been
deprecated.

**Example**

Use `from_kafka` to parse JSON events from a topic:

```tql
from_kafka "events"
this = message.parse_json()
```
