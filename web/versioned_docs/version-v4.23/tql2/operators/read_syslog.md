# read_syslog

Parses an incoming [Syslog](https://en.wikipedia.org/wiki/Syslog) stream into events.

```tql
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

### `merge = bool (optional)`

Merges all incoming events into a single schema\* that converges over time. This
option is usually the fastest *for reading* highly heterogeneous data, but can lead
to huge schemas filled with nulls and imprecise results. Use with caution.

\*: In selector mode, only events with the same selector are merged.

### `raw = bool (optional)`

Use only the raw types that are native to the parsed format. Fields that have a type
specified in the chosen `schema` will still be parsed according to the schema.

In the case of CEF, this means that no parsing of data takes place at all
and every value remains a string, unless the field is in the `schema`.

### `schema = str (optional)`

Provide the name of a [schema](../../data-model/schemas.md) to be used by the
parser.

If a schema with a matching name is installed, the result will always have
all fields from that schema.
* Fields that are specified in the schema, but did not appear in the input will be null.
* Fields that appear in the input, but not in the schema will also be kept. `schema_only=true`
can be used to reject fields that are not in the schema.

If the given schema does not exist, this option instead assigns the output schema name only.

The `schema` option is incompatible with the `selector` option.

### `selector = str (optional)`

Designates a field value as [schema](../../data-model/schemas.md) name with an
optional dot-separated prefix.

The string is parsed as `<fieldname>[:<prefix>]`. The `prefix` is optional and
will be prepended to the field value to generate the schema name.

For example, the Suricata EVE JSON format includes a field
`event_type` that contains the event type. Setting the selector to
`event_type:suricata` causes an event with the value `flow` for the field
`event_type` to map onto the schema `suricata.flow`.

The `selector` option is incompatible with the `schema` option.

### `schema_only = bool (optional)`

When working with an existing schema, this option will ensure that the output
schema has *only* the fields from that schema. If the schema name is obtained via a `selector`
and it does not exist, this has no effect.

This option requires either `schema` or `selector` to be set.

### `unflatten = str (optional)`

A delimiter that, if present in keys, causes values to be treated as values of
nested records.

A popular example of this is the [Zeek JSON](read_zeek_json.md) format. It
includes the fields `id.orig_h`, `id.orig_p`, `id.resp_h`, and `id.resp_p` at
the top-level. The data is best modeled as an `id` record with four nested
fields `orig_h`, `orig_p`, `resp_h`, and `resp_p`.

Without an unflatten separator, the data looks like this:

```tql title="Without unflattening"
{
  id.orig_h: 1.1.1.1,
  id.orig_p: 10,
  id.resp_h: 1.1.1.2,
  id.resp_p: 5,
}
```

With the unflatten separator set to `.`, Tenzir reads the events like this:

```tql title="With 'unflatten'"
{
  id: {
    orig_h: 1.1.1.1,
    orig_p: 10,
    resp_h: 1.1.1.2,
    resp_p: 5,
  }
}
```

## Examples

### Read in the `auth.log`

```tql title="Pipeline"
load_file "/var/log/auth.log"
read_syslog
```

```tql
{
  facility: null,
  severity: null,
  timestamp: 2024-10-14T07:15:01.348027,
  hostname: "tenzirs-magic-machine",
  app_name: "CRON",
  process_id: "895756",
  content: "pam_unix(cron:session): session opened for user root(uid=0) by root(uid=0)",
}
{
  facility: null,
  severity: null,
  timestamp: 2024-10-14T07:15:01.349838,
  hostname: "tenzirs-magic-machine",
  app_name: "CRON",
  process_id: "895756",
  content: "pam_unix(cron:session): session closed for user root"
}
```
