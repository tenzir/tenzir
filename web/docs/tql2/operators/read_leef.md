# read_leef

Parses an incoming [LEEF][leef] stream into events.

```
read_leef [merge=bool, raw=bool, schema=str, selector=str, schema_only=bool, unflatten=str]
```

## Description

The [Log Event Extended Format (LEEF)][leef] is an event representation
popularized by IBM QRadar. Many tools send LEEF over [Syslog](syslog.md).

[leef]: https://www.ibm.com/docs/en/dsm?topic=overview-leef-event-components

LEEF is a line-based format and every line begins with a *header* that is
followed by *attributes* in the form of key-value pairs.

LEEF v1.0 defines 5 header fields and LEEF v2.0 has an additional field to
customize the key-value pair separator, which can be a single character or the
hex value prefixed by `0x` or `x`:

```
LEEF:1.0|Vendor|Product|Version|EventID|
LEEF:2.0|Vendor|Product|Version|EventID|DelimiterCharacter|
```

For LEEF v1.0, the tab (`\t`) character is hard-coded as attribute separator.

Here are some real-world LEEF events:

```
LEEF:1.0|Microsoft|MSExchange|2016|15345|src=10.50.1.1	dst=2.10.20.20	spt=1200
LEEF:2.0|Lancope|StealthWatch|1.0|41|^|src=10.0.1.8^dst=10.0.0.5^sev=5^srcPort=81^dstPort=21
```

Tenzir translates the event attributes into a nested record, where the key-value
pairs map to record fields. Here is an example of the parsed events from above:

```json
{
  "leef_version": "1.0",
  "vendor": "Microsoft",
  "product_name": "MSExchange",
  "product_version": "2016",
  "attributes": {
    "src": "10.50.1.1",
    "dst": "2.10.20.20",
    "spt": 1200,
  }
}
{
  "leef_version": "2.0",
  "vendor": "Lancope",
  "product_name": "StealthWatch",
  "product_version": "1.0",
  "attributes": {
    "src": "10.0.1.8",
    "dst": "10.0.0.5",
    "sev": 5,
    "srcPort": 81,
    "dstPort": 21
  }
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

Read LEEF over a Syslog via UDP:

```
from udp://0.0.0.0:514 read syslog
| parse content leef
| import
```
