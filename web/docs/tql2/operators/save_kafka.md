# save_kafka

Saves a byte stream to a Apache Kafka topic.

```tql
save_kafka [topic=str, key=str, timestamp=time, options=record]

```

## Description

The `save_kafka` operator saves bytes to a Kafka topic.

The implementation uses the official [librdkafka][librdkafka] from Confluent and
supports all [configuration options][librdkafka-options]. You can specify them
via `options` parameter as `{key: value, ...}`. We recommend putting your Kafka options into the
dedicated `kafka.yaml` [plugin config file](../../configuration.md#load-plugins).
This way you can configure your all your environment-specific options once,
independent of the per-connector invocations.

[librdkafka]: https://github.com/confluentinc/librdkafka
[librdkafka-options]: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

The operator injects the following default librdkafka configuration values in
case no configuration file is present, or when the configuration does not
include them:

- `bootstrap.servers`: `localhost`
- `client.id`: `tenzir`
- `group.id`: `tenzir`

<!-- Is still this true? -->
<!-- The default format for the `kafka` connector is `json`. -->

### `topic=str (optional)`

The Kafka topic to use.

Defaults to `"tenzir"`.

### `key=str (optional)`

Sets a fixed key for all messages.

### `timestamp=time (optional)`

Sets a fixed timestamp for all messages.

### `options=record (optional)`

A record of key-value configuration options for
[librdkafka][librdkafka], e.g., `{"auto.offset.reset" : "earliest",
"enable.partition.eof": true}`.

The `save_kafka` operator passes the key-value pairs directly to
[librdkafka][librdkafka]. Consult the list of available [configuration
options][librdkafka-options] to configure Kafka according to your needs.

We recommand factoring these options into the plugin-specific `kafka.yaml` so
that they are indpendent of the `save_kafka` arguments.

## Examples

Write the Tenzir version to topic `tenzir` with timestamp from the past:

```tql
version
write_json
save_kafka timestamp=1984-01-01
```

Follow a CSV file and publish it to topic `data`:

```tql
load_file "/tmp/data.csv"
read_csv
write_json
save_kafka topic="data"
```
