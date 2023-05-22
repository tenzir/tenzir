# json

Reads and writes JSON.

## Synopsis

Parser:

```
json [--selector=field[:prefix]] [--unnest-separator=<string>]
```

Printer:

```
json [--pretty] [--omit-nulls] [--omit-empty-records] [--omit-empty-lists]
     [--omit-empty]
```

## Description

The `json` format provides a parser and printer for JSON and [line-delimited
JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON) objects.

The default loader for the `json` parser is [`stdin`](../connectors/stdin.md).

The default saver for the `json` printer is [`stdout`](../connectors/stdout.md).

### `--selector=field[:prefix]` (Parser)

Designates a field value as schema name, optionally with an added dot-separated
prefix.

For example, the [Suricata EVE JSON](suricata.md) format includes a field
`event_type` that signifies the log type. If we pass
`--selector=event_type:suricata`, a field value of `flow` will create a schema
with the name `suricata.flow`.

### `--unnest-separator=<string>` (Parser)

Designates a string that will unnest the fields if possible.

For example, the [Zeek JSON](zeek-json.md) includes a field `id` in many of
it's events. The `id` is composed of `orig_h`, `orig_p`, `resp_h`,`resp_p`
nested fields. The Zeek JSON outputs the `id` field as a 4 individual key-value
pairs instead of an JSON object. This can be represented as:
```json
{
  "id.orig_h" : "1.1.1.1",
  "id.orig_p" : 10,
  "id.resp_h" : "1.1.1.2",
  "id.resp_p" : 5
}
```

The `--unnest-separator` for Zeek JSON is a `.`.
Each `.` in a field name will be treated as separator between nested fields.
Input in the example above will be transformed into:
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

Sometimes the separator will stay as a part of a field name. For example the:
```json
{
  "cpu" : "cpu01",
  "cpu.logger" : "logger1"
}
```

will not be transformed. The "cpu" field has a a value so it can't be unnested
into a JSON object.

### `--pretty` (Printer)

VAST defaults to line-delimited JSON output (JSONL or NDJSON). The `--pretty`
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
