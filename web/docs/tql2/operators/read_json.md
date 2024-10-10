# read_json

Parses an incoming JSON stream into events.

```
read_json [arrays_of_objects=bool, merge=bool, sep=str,
          schema=str, selector=str, schema_only=bool, raw=bool, unflatten=str]
```

## Description

Parses an incoming JSON stream into events.

### arrays_of_objects

Parse arrays of objects, with every object in the outermost arrays resulting in
one event each. This is particularly useful when interfacing with REST APIs,
which often yield large arrays of objects instead of newline-delimited JSON
objects.

### `merge`

Merges all incoming events into a single schema\* that converges over time. This
option is usually the fastest *for reading* highly heterogeneous data, but can
lead
to huge schemas filled with nulls and imprecise results. Use with caution.

\*: In selector mode, only events with the same selector are merged.

This option can not be combined with `--raw --schema`.

### `raw`

Use only the raw JSON types. This means that all strings are parsed as `string`,
irrespective of whether they are a valid `ip`, `duration`, etc. Also, since JSON
only has one generic number type, all numbers are parsed with the `double` type.

### sep
TODO

### `schema`
Provide the name of a [schema](../data-model/schemas.md) to be used by the
parser. If the schema uses the `blob` type, then the JSON parser expects
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
read_json

load_file "events.json"
read_json schema=...

...
```
