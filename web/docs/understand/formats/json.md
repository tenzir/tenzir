# json

Reads and writes JSON.

## Synopsis

Parser:

```
json [--selector=field[:prefix]]
```

Printer:

```
json [--pretty] [--numeric-durations] 
     [--omit-nulls] [--omit-empty-records] [--omit-empty-lists] [--omit-empty]
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

### `--pretty` (Printer)

VAST defaults to line-delimited JSON output (JSONL or NDJSON). The `--pretty`
flag switches to a tree-structured representation.

### `--numeric-durations` (Printer)

Render durations as fractional seconds (like Unix timestamps, e.g.,
`62.12`) as opposed to strings with unit (e.g., `1.04 mins`).

This comes in handy when downstream processing involves arithmetic on time
durations. Performing this with the SI suffix notation would require addional
parsing.

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
