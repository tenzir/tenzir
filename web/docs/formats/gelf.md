---
sidebar_custom_props:
  format:
    parser: true
---

# gelf

Reads Graylog Extended Log Format (GELF) events.

## Synopsis

```
gelf
```

## Description

The `gelf` parser reads events formatted in [Graylog Extended Log Format
(GELF)][gelf-spec], a format that predominantly
[Graylog](../integrations/graylog.md) uses for importing and exporting
of structured data.

Tenzir parses GELF as a stream of NDJSON records separated by a `\0` byte. GELF
messages can also occur one at a time, e.g., in a UDP packet or Kafka message.
In this case there is no separator. The [chunked mode](#chunked-mode) allows for
splitting large messages into smaller fragments.

[gelf-spec]: https://go2docs.graylog.org/5-0/getting_in_log_data/gelf.html

According to version 1.1 of the specification, a GELF message has the following
structure:

| Field               | Type           | Description                              | Requirement |
|---------------------|----------------|------------------------------------------|:-----------:|
| `version`           | string         | GELF spec version: `"1.1"`               | ✅          |
| `host`              | string         | Host, source, or application name        | ✅          |
| `short_message`     | string         | A short descriptive message              | ✅          |
| `full_message`      | string         | Long message, possibly with a backtrace  | ➖          |
| `timestamp`         | number         | UNIX epoch seconds; optional milliseconds | ➖         |
| `level`             | number         | Standard syslog level, defaults to 1     | ➖          |
| `facility`          | string         | Message tag                              | ❌          |
| `linenumber`        | number         | Line causing the error                   | ❌          |
| `file`              | string         | File causing the error                   | ❌          |
| _[additional field] | string / number | User-defined data                       | ➖          |

The requirement column defines whether a field is mandatory (✅), optional (➖),
or deprecated (❌).

Here is an example GELF message:

```json
{
  "version": "1.1",
  "host": "example",
  "short_message": "TL;DR",
  "full_message": "The whole enchilada",
  "timestamp": 1385053862.3072,
  "level": 1,
  "_user_id": 1337,
  "_gl2_remote_ip": "6.6.6.6",
}
```

By convention, Graylog uses the `_gl2_` prefix for its own field. There is no
formalized convention for naming, and exact field names may depend on your
configuration.

:::caution Boolean values
Graylog's implementation of GELF does not support boolean values and [drops them
on ingest](https://github.com/Graylog2/graylog2-server/issues/5504).
:::

### Chunked mode

Because a single GELF message can exceed the capacity of the underlying frame,
GELF also supports a *chunked mode* where a single message can be split into at
most 128 chunks, each of which have the following header:

1. **GELF magic bytes** (2 bytes): `0x1e 0x0f`
2. **Message ID** (8 bytes): A unique ID for all chunks in the same message
3. **Sequence number** (1 byte): Intra-chunk counter starting at 0
4. **Sequence count** (1 byte): Total number of chunks of the message

Graylog implementations mandate that all chunks must arrive within 5 seconds,
otherwise all chunks with the given message ID will be discarded.

:::caution No support for chunked mode
Tenzir currently does not support chunked mode. Please [reach out](/discord) if
you would like to see support in future versions.
:::

## Examples

Accept GELF from a [TCP](../connectors/tcp.md) socket:

```
from tcp://1.2.3.4 read gelf
```

Read GELF from [Kafka](../connectors/kafka.md) at the `graylog` topic:

```
from kafka --topic graylog read gelf
```
