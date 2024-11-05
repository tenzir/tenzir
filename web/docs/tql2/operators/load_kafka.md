# load_kafka

Loads a byte stream from a Apache Kafka topic.

```tql
load_kafka [topic=str, count=int, exit=bool, offset=str, options=record]
```

## Description

The `load_kafka` operator reads bytes from a Kafka topic.

The implementation uses the official [librdkafka][librdkafka] from Confluent and
supports all [configuration options][librdkafka-options]. You can specify them
via `options` parameter as `{key: value, ...}`.

We recommend putting your Kafka options into the dedicated `kafka.yaml` [plugin
config file](../../configuration.md#load-plugins). This way you can configure
your all your environment-specific options once, independent of the
per-connector invocations.

[librdkafka]: https://github.com/confluentinc/librdkafka
[librdkafka-options]: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

The operator injects the following default librdkafka configuration values in
case no configuration file is present, or when the configuration does not
include them:

- `bootstrap.servers`: `localhost`
- `client.id`: `tenzir`
- `group.id`: `tenzir`

### `topic=str (optional)`

The Kafka topic to use.

Defaults to `"tenzir"`.

### `count=int (optional)`

Exit successfully after having consumed `count` messages.

### `exit=bool (optional)`

Exit successfully after having received the last message.

Without this option, the operator waits for new messages after having consumed the
last one.

### `offset=str (optional)`

The offset to start consuming from. Possible values are:

- `beginning`: first offset
- `end`: last offset
- `stored`: stored offset
- `<value>`: absolute offset
- `-<value>`: relative offset from end

<!--
- `s@<value>`: timestamp in ms to start at
- `e@<value>`: timestamp in ms to stop at (not included)
-->

### `options=record (optional)`

A record of key-value configuration options for
[librdkafka][librdkafka], e.g., `{"auto.offset.reset" : "earliest",
"enable.partition.eof": true}`.

The `load_kafka` operator passes the key-value pairs directly to
[librdkafka][librdkafka]. Consult the list of available [configuration
options][librdkafka-options] to configure Kafka according to your needs.

We recommand factoring these options into the plugin-specific `kafka.yaml` so
that they are indpendent of the `load_kafka` arguments.

## Examples

### Read 100 JSON messages from the topic `tenzir`

```tql
load_kafka count=100
read_json
```

### Read Zeek Streaming JSON logs starting at the beginning

```tql
load_kafka topic="zeek", offset="beginning"
read_zeek_json
```
