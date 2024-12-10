# Kafka

[Apache Kafka](https://kafka.apache.org) is a distributed open-source message
broker. The Tenzir integration can publish (send messages to a topic) or
subscribe (receive) messages from a topic.

![Kafka Diagram](kafka.svg)

Internally, we use Confluent's official
[librdkafka](https://github.com/confluentinc/librdkafka) library, which gives us
full control in passing options.

:::tip URL Support
The URL scheme `kafka://` dispatches to
[`load_kafka`](../../tql2/operators/load_kafka.md) and
[`save_kafka`](../../tql2/operators/save_kafka.md) for seamless URL-style use via
[`from`](../../tql2/operators/from.md) and [`to`](../../tql2/operators/to.md).
:::

## Examples

### Send events to a Kafka topic

```tql
from {
  x: 42,
  y: "foo",
}
to "kafka://topic" {
  write_ndjson
}
```

### Subscribe to a topic

The `offset` option controls where to start reading:

```tql
from "kafka://topic", offset="beginning" {
  read_ndjson
}
```

Other values are `"end"` to read at the last offset, `"stored"` to read at the
stored offset, a positive integer representing an absolute offset, or a negative
integer representing a relative offset counting from the end.
