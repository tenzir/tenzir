# read_ssv

Read SSV (Space-Separated Values) from a byte stream.

```
read_ssv [list_sep=str, null_value=str, comments=bool, header=str, auto_expand=bool,
          schema=str, selector=str, schema_only=bool, raw=bool, unflatten=str]
```

## Description

The `read_ssv` operator transforms a byte stream into a event stream by parsing
the bytes as SSV.

### `auto_expand`
Automatically add fields to the schema when encountering events with too many
values instead of dropping the excess values.

### `comments`
Treat lines beginning with "#" as comments.

### `header`
The `string` to be used as a `header` for the parsed values.
If unspecified, the first line of the input is used as the header.

### `list_sep`
The `string` separating the elements _inside_ a list.

Defaults to `,`.

### `null_value`
The `string` denoting an absent value.

Defaults to `-`.

### `raw`
Use only the raw types that are native to the parsed format. Fields that have a type
specified in the chosen schema will still be parsed according to the schema.

For example, the JSON format has no notion of an IP address, so this will cause all IP addresses
to be parsed as strings, unless the field is specified to be an IP address by the schema.
JSON however has numeric types, so those would be parsed.

Use with caution.

This option can not be combined with `--merge --schema`.

### `schema`
Provide the name of a [schema](../../data-model/schemas.md) to be used by the
parser. If the schema uses the `blob` type, then the JSON parser expects
base64-encoded strings.

The `schema` option is incompatible with the `selector` option.

### `selector`
Designates a field value as schema name with an optional dot-separated prefix.

For example, the Suricata EVE JSON format includes a field
`event_type` that contains the event type. Setting the selector to
`event_type:suricata` causes an event with the value `flow` for the field
`event_type` to map onto the schema `suricata.flow`.

The `selector` option is incompatible with the `schema` option.

### `schema_only`
When working with an existing schema, this option will ensure that the output
schema has *only* the fields from that schema. If the schema name is obtained via a `selector`
and it does not exist, this has no effect.

This option requires either `schema` or `selector` to be set.

### `unflatten`

## Examples

