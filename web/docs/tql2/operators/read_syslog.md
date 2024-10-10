# read_syslog

Parses an incoming [Syslog](https://en.wikipedia.org/wiki/Syslog) stream into events.

```
read_syslog [merge=bool, raw=bool, schema=str, selector=str, schema_only=bool, unflatten=str]
```

## Description

[Syslog](https://en.wikipedia.org/wiki/Syslog) is a standard format for message logging.

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

### `merge`

Merges all incoming events into a single schema\* that converges over time. This
option is usually the fastest *for reading* highly heterogeneous data, but can lead
to huge schemas filled with nulls and imprecise results. Use with caution.

\*: In selector mode, only events with the same selector are merged.

This option can not be combined with `--raw --schema`.

### `raw`

Use only the raw types that are native to the parsed format. Fields that have a type
specified in the chosen schema will still be parsed according to the schema.

For example, the JSON format has no notion of an IP address, so this will cause all IP addresses
to be parsed as strings, unless the field is specified to be an IP address by the schema.
JSON however has numeric types, so those would be parsed.

Use with caution.

### `schema`
Provide the name of a [schema](../data-model/schemas.md) to be used by the
parser. If the schema uses the `blob` type, then the Syslog parser expects
base64-encoded strings.

The `schema` option is incompatible with the `selector` option.

### `selector`
Designates a field value as schema name with an optional dot-separated prefix.

For example, the [Suricata EVE JSON](suricata.md) format includes a field
`event_type` that contains the event type. Setting the selector to
`event_type:suricata` causes an event with the value `flow` for the field
`event_type` to map onto the schema `suricata.flow`.

The `selector` option is incompatible with the `schema` option.

### `schema_only`
When working with an existing schema, this option will ensure that the output
schema has *only* the fields from that schema. If the schema name is obtained via a `selector`
and it does not exist, this has no effect.

This option requires either `schema` or `selector` to be set.

### unflatten

## Examples

```
load_file "events.json"
read_syslog

load_file "events.json"
read_syslog schema=...

...
```
