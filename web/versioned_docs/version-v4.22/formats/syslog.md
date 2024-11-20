---
sidebar_custom_props:
  format:
    parser: true
---

# syslog

Reads syslog messages.

## Synopsis

```
syslog [--schema <schema>] [--selector <selector>] [--schema-only]
       [--merge] [--raw] [--unnest-separator <nested-key-separator>]
```

## Description

Syslog is a standard format for message logging.
Tenzir supports reading syslog messages in both the standardized "Syslog Protocol" format
([RFC 5424](https://tools.ietf.org/html/rfc5424)), and the older "BSD syslog Protocol" format
([RFC 3164](https://tools.ietf.org/html/rfc3164)).

Depending on the syslog format, the result can be different.
Here's an example of a syslog message in RFC 5424 format:

```
<165>8 2023-10-11T22:14:15.003Z mymachineexamplecom evntslog 1370 ID47 [exampleSDID@32473 eventSource="Application" eventID="1011"] Event log entry
```

With this input, the parser will produce the following output, with the schema name `syslog.rfc5424`:

```json
{
  "facility": 20,
  "severity": 5,
  "version": 8,
  "timestamp": "2023-10-11T22:14:15.003000",
  "hostname": "mymachineexamplecom",
  "app_name": "evntslog",
  "process_id": "1370",
  "message_id": "ID47",
  "structured_data": {
    "exampleSDID@32473": {
      "eventSource": "Application",
      "eventID": 1011
    }
  },
  "message": "Event log entry"
}
```

Here's an example of a syslog message in RFC 3164 format:

```
<34>Nov 16 14:55:56 mymachine PROGRAM: Freeform message
```

With this input, the parser will produce the following output, with the schema name `syslog.rfc3164`:

```json
{
  "facility": 4,
  "severity": 2,
  "timestamp": "Nov 16 14:55:56",
  "hostname": "mymachine",
  "app_name": "PROGRAM",
  "process_id": null,
  "content": "Freeform message"
}
```

### Common Options (Parser)

The syslog parser supports the common [schema inference options](formats.md#parser-schema-inference).

## Examples

Read a syslog file:

```
from mylog.log read syslog
```
