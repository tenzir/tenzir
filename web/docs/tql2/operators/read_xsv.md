# read_xsv

Read XSV (`X`-Separated Values) from a byte stream.

```tql
read_xsv field_sep:str, list_sep:str, null_value:str, 
          [comments=bool, header=str, auto_expand=bool, schema=str, selector=str, 
          schema_only=bool, raw=bool, unflatten=str]
```

## Description

The `read_xsv` operator transforms a byte stream into a event stream by parsing
the bytes as [XSV](https://en.wikipedia.org/wiki/Delimiter-separated_values).

### `field_sep: str (optional)`

The `string` separating different fields.

### `list_sep: str (optional)`

The `string` separating different elements in a list.

### `null_value: str (optional)`

The `string` denoting an absent value.

### `auto_expand = bool (optional)`

Automatically add fields to the schema when encountering events with too many
values instead of dropping the excess values.

### `comments = bool (optional)`

Treat lines beginning with "#" as comments.

### `header = str (optional)`

The `string` to be used as a `header` for the parsed values.
If unspecified, the first line of the input is used as the header.

### `list_sep = str (optional)`

The `string` separating the elements _inside_ a list.

### `raw = bool (optional)`

Use only the raw types that are native to the parsed format. Fields that have a type
specified in the chosen schema will still be parsed according to the schema.

For example, the JSON format has no notion of an IP address, so this will cause all IP addresses
to be parsed as strings, unless the field is specified to be an IP address by the schema.
JSON however has numeric types, so those would be parsed.

Use with caution.

This option can not be combined with `merge=true, schema=<schema>`.

### `schema = str (optional)`

Provide the name of a [schema](../../data-model/schemas.md) to be used by the
parser. If the schema uses the `blob` type, then the JSON parser expects
base64-encoded strings.

The `schema` option is incompatible with the `selector` option.

### `selector = str (optional)`

Designates a field value as schema name with an optional dot-separated prefix.

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

```json
{
  "id.orig_h" : "1.1.1.1",
  "id.orig_p" : 10,
  "id.resp_h" : "1.1.1.2",
  "id.resp_p" : 5
}
```

With the unflatten separator set to `.`, Tenzir reads the events like this:

```json
{
  "id" : {
    "orig_h" : "1.1.1.1",
    "orig_p" : 10,
    "resp_h" : "1.1.1.2",
    "resp_p" : 5
  }
}
```

## Examples

