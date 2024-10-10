# read_kv

Read Key-Value pairs from a byte stream.

```
read_kv [field_split:str, value_split:str, merge=bool, raw=bool, schema=str, selector=str, schema_only=bool, unflatten=str]
```

## Description

The `read_kv` operator transforms a byte stream into a event stream by parsing
the bytes as Key-Value pairs.
Incoming strings are first split into fields according to `<field_split>`. This
can be a regular expression. For example, the input `foo: bar, baz: 42` can be
split into `foo: bar` and `baz: 42` with the `",\s*"` (a comma, followed by any
amount of whitespace) as the field splitter. Note that the matched separators
are removed when splitting a string.

Afterwards, the extracted fields are split into their key and value by
`<value_split>`, which can again be a regular expression. In our example,
`":\s*"` could be used to split `foo: bar` into the key `foo` and its value
`bar`, and similarly `baz: 42` into `baz` and `42`. The result would thus be
`{"foo": "bar", "baz": 42}`. If the regex matches multiple substrings, only the
first match is used.

The supported regular expression syntax is
[RE2](https://github.com/google/re2/wiki/Syntax). In particular, this means that
lookahead `(?=...)` and lookbehind `(?<=...)` are not supported by `kv` at
the moment. However, if the regular expression has a capture group, it is
assumed
that only the content of the capture group shall be used as the separator. This
means that unsupported regular expressions such as `(?=foo)bar(?<=baz)` can be
effectively expressed as `foo(bar)baz` instead.

### Quoted Values

The parser is aware of double-quotes (`"`). If the `<field_split>` or
`<value_split>` are found within enclosing quotes, they are not considered
matches.

This means that both the key and the value may be enclosed in double-quotes.

For example, given `\s*,\s*` and `=`, the input

```
"key"="nested = value",key2="value, and more"
```
will parse as
```json
{
  "key" : "nested = value"
}
{
  "key2" : "value, and more"
}
  ```

### `<field_split>`

The regular expression used to separate individual fields. 

Defaults to `\s`.

### `<value_split>`

The regular expression used to separate a key from its value. 

Defaults to `=`

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

XXX: Do both need to specified to fail?
This option can not be combined with `--merge --schema`.

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

### `unflatten`

## Examples

Extract comma-separated key-value pairs from `foo:1, bar:2,baz:3 , qux:4`:

```
kv "\s*,\s*" ":"
```

Extract key-value pairs from strings such as `FOO: C:\foo BAR_BAZ: hello world`.
This requires lookahead because the fields are separated by whitespace, but not
every whitespace acts as a field separator. Instead, we only want to split if
the whitespace is followed by `[A-Z][A-Z_]+:`, i.e., at least two uppercase
characters followed by a colon. We can express this as `"(\s+)[A-Z][A-Z_]+:"`,
which yields `FOO: C:\foo` and `BAR_BAZ: hello world`. We then split the key
from its value with `":\s*"` (only the first match is used to split them). The
final result is thus `{"FOO": "C:\foo", "BAR_BAZ": "hello world"}`.

```
kv "(\s+)[A-Z][A-Z_]+:" ":\s*"
```
