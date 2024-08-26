---
sidebar_custom_props:
  format:
    parser: true
---

# gelf

Reads Graylog Extended Log Format (GELF) events.

## Synopsis
Parser:
```
gelf [--merge] [--schema <schema>] [--selector <fieldname[:prefix]>]
     [--expand-schema] [--raw] [--unnest-separator <separator>]
```

## Description

The `gelf` parser reads events formatted in [Graylog Extended Log Format
(GELF)][gelf-spec], a format that predominantly
[Graylog](../integrations/graylog.md) uses for importing and exporting
of structured data.

Tenzir parses GELF as a stream of JSON records separated by a `\0` byte. GELF
messages can also occur one at a time (e.g., framed in a HTTP body, UDP packet,
or Kafka message) in which case there is no separator.

GELF also supports a *chunked mode* where a single message can be split into at
most 128 chunks. Tenzir currently does not support this mode. Please [reach
out](/discord) if you would like to see support in future versions.

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

By convention, Graylog uses the `_gl2_` prefix for its own fields. There is no
formalized convention for naming, and exact field names may depend on your
configuration.

:::caution Boolean values
Graylog's implementation of GELF does not support boolean values and [drops them
on ingest](https://github.com/Graylog2/graylog2-server/issues/5504).
:::

### Common Options (Parser)

The GELF parser supports the common [schema inference options](formats.md#parser-schema-inference).

## Examples

Accept GELF from a [TCP](../connectors/tcp.md) socket:

```
from tcp://1.2.3.4 read gelf
```

Read GELF messages from [Kafka](../connectors/kafka.md) from the `graylog`
topic:

```
from kafka --topic graylog read gelf
```
