# kafka

Loads bytes from and saves bytes to Kafka.

## Synopsis

Loader:

```
kafka -t <topic> [-o|--option=<key=value>]
```

Saver:

```
kafka -t <topic>
```

## Description

The `kafka` loaders reads bytes from a Kafka topic. The `kafka` saver writes
bytes to a Kafka topic.

Tenzir uses the official [librdkafka][librdkafka] from Confluent.

[librdkafka]: https://github.com/confluentinc/librdkafka

The default format for the `kafka` connector is [`json`](../formats/json.md).

### `<topic>` (Loader, Saver)

The Kafka topic use.

### `<key=value>` (Loader, Saver)

A key-value configuration option for [librdkafka][librdkafka].

The `kafka` operator passes key and value directly to [librdkafka][librdkafka].
Consult the list of available [configuration options][librdkafka-options] to
configure Kafka according to your needs.

[librdkafka-options]: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

## Examples

Read JSON messages from the topic `tenzir`:

```
from kafka -t tenzir read json
```

Read Zeek Streaming JSON logs from topic `zeek`:

```
from kafka -t zeek read zeek-json
```

Write the Tenzir version to topic `tenzir`:

```
version | to kafka -t tenzir
```

Follow a CSV file and publish it to topic `data`:

```
from file -f /tmp/data.csv read csv | to kafka -t data
```
