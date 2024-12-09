---
sidebar_custom_props:
  connector:
    loader: true
    saver: true
---

# kafka

Loads bytes from and saves bytes to Kafka.

## Synopsis

Loader:

```
kafka [-t <topic>] [-c|--count <n>] [-e|--exit] [-o|--offset <offset>]
      [-X|--set <key=value>,...]
```

Saver:

```
kafka [-t <topic>] [-k|--key <key>] [-T|--timestamp <time>]
      [-X|--set <key=value>]

```

## Description

The `kafka` loader reads bytes from a Kafka topic. The `kafka` saver writes
bytes to a Kafka topic.

The implementation uses the official [librdkafka][librdkafka] from Confluent and
supports all [configuration options][librdkafka-options]. You can specify them
via `-X <key=value>,...`. We recommend putting your Kafka options into the
dedicated `kafka.yaml` [plugin config file](../configuration.md#load-plugins).
This way you can configure your all your environment-specific options once,
independent of the per-connector invocations.

[librdkafka]: https://github.com/confluentinc/librdkafka
[librdkafka-options]: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

The connector injects the following default librdkafka configuration values in
case no configuration file is present, or when the configuration does not
include them:

- `bootstrap.servers`: `localhost`
- `client.id`: `tenzir`
- `group.id`: `tenzir`

The default format for the `kafka` connector is [`json`](../formats/json.md).

### `-t|--topic <topic>` (Loader, Saver)

The Kafka topic use.

Defaults to `tenzir`.

### `-c|--count <n>` (Loader)

Exit successfully after having consumed `n` messages.

### `-e|--exit` (Loader)

Exit successfully after having received the last message.

Without this option, the loader waits for new messages after having consumed the
last one.

### `-o|--offset <offset>` (Loader)

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

### `-X|--set <key=value>` (Loader, Saver)

A comma-separated list of key-value configuration options for
[librdkafka][librdkafka], e.g., `-X
auto.offset.reset=earliest,enable.partition.eof=true`.

The `kafka` operator passes the key-value pairs directly to
[librdkafka][librdkafka]. Consult the list of available [configuration
options][librdkafka-options] to configure Kafka according to your needs.

We recommand factoring these options into the plugin-specific `kafka.yaml` so
that they are indpendent of the `kafka` connector arguments.

### `-k|--key <key>` (Saver)

Sets a fixed key for all messages.

### `-T|--timestamp <time>` (Saver)

Sets a fixed timestamp for all messages.

## Examples

Read 100 JSON messages from the topic `tenzir`:

```
from kafka -c 100 read json
```

Read Zeek Streaming JSON logs from topic `zeek` starting at the beginning:

```
from kafka -t zeek -o beginning read zeek-json
```

Write the Tenzir version to topic `tenzir` with timestamp from the past:

```
version | to kafka -T 1984-01-01
```

Follow a CSV file and publish it to topic `data`:

```
from file -f /tmp/data.csv read csv | to kafka -t data
```
