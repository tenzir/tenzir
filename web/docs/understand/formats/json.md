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

A delimiter that, if present in keys, causes values to be treated as values of nested records.

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

With the unnest separator set to `.`, VAST reads the events like this:

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
