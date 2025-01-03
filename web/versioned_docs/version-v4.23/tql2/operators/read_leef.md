# read_leef

Parses an incoming [LEEF][leef] stream into events.

```tql
read_leef [merge=bool, raw=bool, schema=str, selector=str, schema_only=bool, unflatten=str]
```

## Description

The [Log Event Extended Format (LEEF)][leef] is an event representation
popularized by IBM QRadar. Many tools send LEEF over [Syslog](read_syslog.md).

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

### `merge = bool (optional)`

Merges all incoming events into a single schema\* that converges over time. This
option is usually the fastest *for reading* highly heterogeneous data, but can lead
to huge schemas filled with nulls and imprecise results. Use with caution.

\*: In selector mode, only events with the same selector are merged.

### `raw = bool (optional)`

Use only the raw types that are native to the parsed format.
In the case of LEEF this means that no parsing of data takes place at all
and every value remains a string.

If a known `schema` is given, fields will still be parsed according to the schema.

Use with caution.

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

A popular example of this is the [Zeek JSON](read_zeek_json.md) format. It includes
the fields `id.orig_h`, `id.orig_p`, `id.resp_h`, and `id.resp_p` at the
top-level. The data is best modeled as an `id` record with four nested fields
`orig_h`, `orig_p`, `resp_h`, and `resp_p`.

Without an unflatten separator, the data looks like this:

```tql title="Without unflattening"
{
  id.orig_h: 1.1.1.1,
  id.orig_p: 10,
  id.resp_h: 1.1.1.2,
  id.resp_p: 5
}
```

With the unflatten separator set to `.`, Tenzir reads the events like this:

```tql title="With 'unflatten'"
{
  id: {
    orig_h: 1.1.1.1,
    orig_p: 10,
    resp_h: 1.1.1.2,
    resp_p: 5
  }
}
```

## See Also

[read_cef](read_cef.md)
