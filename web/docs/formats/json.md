# json

Reads and writes JSON.

## Synopsis

Parser:

```
json [--selector=field[:prefix]] [--unnest-separator=<string>] [--no-infer]
     [--ndjson]
```

Printer:

```
json [--pretty] [--omit-nulls] [--omit-empty-records] [--omit-empty-lists]
     [--omit-empty]
```

## Description

The `json` format provides a parser and printer for JSON and [line-delimited
JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON) objects.

### Type inference (Parser)

By default the `parser` will infer the types of a fields in the input events.
This behaviour can be switched off with a usage of [--no-infer](#--no-infer)
option. This means that the input doesn't require a `schema`. All the types will
be inferred base on the input data. It is also possible for a field to
represent a different type in different events. E.g.

```json
{
  "a" : "string"
}
{
  "a" : 5
}
```

In such case the `parser` will try to `cast` the `5` into a first inferred type
(`string` in this particular case). This conversion is allowed, so the ouput of
parsing this data would be:

```json
{
  "a" : "string"
}
{
  "a" : "5"
}
```

It is also possible for the types to not be compatible. E.g.

```json
{
  "a" : "10ns"
}
{
  "a" : "20ns"
}
{
  "a" : true
}
```

The first inferred type would be a `duration` and the second one a `bool`.
In this case the `"a"` field will be cast into a type that can represent both
types (`common type`), which is a `string` type in this particular case. The output would be:

```json
{
  "a" : "10ns"
}
{
  "a" : "20ns"
}
{
  "a" : "true"
}
```

This logic also applies when the schema is specified. All of the input values
that don't match the schema's field type will be cast to it. The `common type`
logic will also be used when the schema's type is not compatible with the field
type.

### `--selector=field[:prefix]` (Parser)

Designates a field value as schema name, optionally with an added dot-separated
prefix.

For example, the [Suricata EVE JSON](suricata.md) format includes a field
`event_type` that signifies the log type. If we pass
`--selector=event_type:suricata`, a field value of `flow` will create a schema
with the name `suricata.flow`.

### `--unnest-separator=<string>` (Parser)

A delimiter that, if present in keys, causes values to be treated as values of
nested records.

A popular example of this is the [Zeek JSON](zeek-json.md) format. It includes
the fields `id.orig_h`, `id.orig_p`, `id.resp_h`, and `id.resp_p` at the
top-level. The data is best modeled as an `id` record with four nested fields
`orig_h`, `orig_p`, `resp_h`, and `resp_p`.

Without an unnest separator, the data looks like this:

```json
{
  "id.orig_h" : "1.1.1.1",
  "id.orig_p" : 10,
  "id.resp_h" : "1.1.1.2",
  "id.resp_p" : 5
}
```

With the unnest separator set to `.`, Tenzir reads the events like this:

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

### `--no-infer` (Parser)

This option only has an effect when combined with [--selector=field[:prefix]](#--selector=field[:prefix]).
Each field in the input events must have a type that matches the selected schema.
The type mismatch will emit a warning and the data will be discarded.
Lack of selector field or a schema pointed by this field will result in
discarding of the whole event.

### `--ndjson` (Parser)

Makes the parser treat the data as NDJSON.
Each line must contain only one event. Multiple events in a single line e.g.

```json
{"foo": "baz"}{"foo": "qux"}
```

will result in a parse error and these events will be discarded.
Events that span over multiple lines will also emit an error and the events
will be discarded.

Example:
```json
{"foo":
{"bar": "baz"}
}
```

### `--pretty` (Printer)

Tenzir defaults to line-delimited JSON output (JSONL or NDJSON). The `--pretty`
flag switches to a tree-structured representation.

### `--omit-nulls` (Printer)

Strips `null` fields from the input.

Example:

```json
{
  "a": null
  "b": [42, null, 43],
  "c": {
    "d": null
    "e": 42
  }
}
```

With `--omit-nulls`, this example becomes:

```json
{
  "b": 42,
  "c": {
    "e": 42
  }
}
```

### `--omit-empty-records` (Printer)

Strips empty records from the input.

Example:

```json
{
  "w": null
  "x": {},
  "y": {
    "z": {}
  }
}
```

With `--omit-empty-records`, this example becomes:

```json
{
  "w": 42,
}
```

### `--omit-empty-lists` (Printer)

Strips empty lists from the input.

Example:

```json
{
  "m": []
  "n": {},
  "o": {
    "p": []
  }
}
```

With `--omit-empty-lists`, this example becomes:

```json
{
  "m": {},
  "o": {}
}
```

### `--omit-empty` (Printer)

This options combines all other `--omit-*` options.

## Examples

Read JSON from stdin, either NDJSON or tree-structured:

```
read json
```

Write compact JSON without empty fields to a file:

```
write json --omit-empty to file /tmp/result.json
```

Pretty-print JSON in tree-structured form to stdout:

```
write json --pretty
```
