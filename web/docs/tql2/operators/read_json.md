# read_json

:::tip
If you are receiving newline-delimited JSON (NDJSON), use [`read_ndjson`](read_ndjson) instead.
:::

Parses an incoming JSON stream into events.

```tql
read_json [schema=str, selector=str, schema_only=bool, merge=bool, raw=bool,
           unflatten=str, arrays_of_objects=bool]
```


## Description

Parses an incoming JSON byte stream into events.

### `arrays_of_objects = bool (optional)`

Default: `false`.

Parse arrays of objects, with every object in the outermost arrays resulting in
one event each. This is particularly useful when interfacing with REST APIs,
which often yield large arrays of objects instead of newline-delimited JSON
objects.

### `merge = bool (optional)`

Merges all incoming events into a single schema\* that converges over time. This
option is usually the fastest *for reading* highly heterogeneous data, but can
lead
to huge schemas filled with nulls and imprecise results. Use with caution.

\*: In selector mode, only events with the same selector are merged.

### `raw = bool (optional)`

Use only the raw types that are native to the parsed format. Fields that have a type
specified in the chosen `schema` will still be parsed according to the schema.

This means that JSON numbers will be parsed as numbers,
but every JSON string remains a string, unless the field is in the `schema`.

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

The string is parsed as `<filename>[:<prefix>]`. The `prefix` is optional and
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

```json title="Without unflattening"
{
  "id.orig_h": "1.1.1.1",
  "id.orig_p": 10,
  "id.resp_h": "1.1.1.2",
  "id.resp_p": 5
}
```

With the unflatten separator set to `.`, Tenzir reads the events like this:

```json title="With 'unflatten'"
{
  "id": {
    "orig_h": "1.1.1.1",
    "orig_p": 10,
    "resp_h": "1.1.1.2",
    "resp_p": 5
  }
}
```

## Examples


### Read a JSON file

```json title="input.json"
{
  "product": "Tenzir",
  "version.major": 4,
  "version.minor": 22
}
{
  "product": "Tenzir",
  "version.major": 4,
  "version.minor": 21,
  "version.dirty": true
}
```

```tql title="Pipeline"
load_file "events.json"
read_json unflatten="."
```

```json title="Output"
{
  "product": "Tenzir",
  "version": {
    "major": 4,
    "minor": 22
  }
}
{
  "product": "Tenzir",
  "version": {
    "major": 4,
    "minor": 21,
    "dirty": true
  }
}
```

### Read a JSON array

[JA4+](https://ja4db.com/) provides fingerprints via a REST API, which returns a single JSON array.

<details>
<summary>Example Input</summary>

```json
[
  {
    "application": "SemrushBot",
    "library": null,
    "device": null,
    "os": "Other",
    "user_agent_string": null,
    "certificate_authority": null,
    "observation_count": 449,
    "verified": false,
    "notes": null,
    "ja4_fingerprint": "t13d301000_01455d0db58d_5ac7197df9d2",
    "ja4_fingerprint_string": null,
    "ja4s_fingerprint": null,
    "ja4h_fingerprint": "ge11nn100000_c910c42e1704_e3b0c44298fc_e3b0c44298fc",
    "ja4x_fingerprint": null,
    "ja4t_fingerprint": null,
    "ja4ts_fingerprint": null,
    "ja4tscan_fingerprint": null
  },
  {
    "application": null,
    "library": null,
    "device": "Epson Printer",
    "os": null,
    "user_agent_string": null,
    "certificate_authority": null,
    "observation_count": 1,
    "verified": true,
    "notes": null,
    "ja4_fingerprint": null,
    "ja4s_fingerprint": null,
    "ja4h_fingerprint": null,
    "ja4x_fingerprint": null,
    "ja4t_fingerprint": null,
    "ja4ts_fingerprint": null,
    "ja4tscan_fingerprint": "28960_2-4-8-1-3_1460_3_1-4-8-16"
  },
  ...
]
```

</details>

You can easily ingest this into Tenzir using

```tql title="Pipeline"
load "https://ja4db.com/api/read/"
read_json arrays_of_objects=true
```

```json title="Example Output"
{
  "application": "SemrushBot",
  "library": null,
  "device": null,
  "os": "Other",
  "user_agent_string": null,
  "certificate_authority": null,
  "observation_count": 449,
  "verified": false,
  "notes": null,
  "ja4_fingerprint": "t13d301000_01455d0db58d_5ac7197df9d2",
  "ja4_fingerprint_string": null,
  "ja4s_fingerprint": null,
  "ja4h_fingerprint": "ge11nn100000_c910c42e1704_e3b0c44298fc_e3b0c44298fc",
  "ja4x_fingerprint": null,
  "ja4t_fingerprint": null,
  "ja4ts_fingerprint": null,
  "ja4tscan_fingerprint": null
},
{
  "application": null,
  "library": null,
  "device": "Epson Printer",
  "os": null,
  "user_agent_string": null,
  "certificate_authority": null,
  "observation_count": 1,
  "verified": true,
  "notes": null,
  "ja4_fingerprint": null,
  "ja4s_fingerprint": null,
  "ja4h_fingerprint": null,
  "ja4x_fingerprint": null,
  "ja4t_fingerprint": null,
  "ja4ts_fingerprint": null,
  "ja4tscan_fingerprint": "28960_2-4-8-1-3_1460_3_1-4-8-16"
}
```
