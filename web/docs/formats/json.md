# json

Reads and writes JSON.

## Synopsis

Parser:

```
json [--schema=<schema>] [--selector=<field[:prefix]>] [--unnest-separator=<string>]
     [--no-infer] [--ndjson]
```

Printer:

```
json [--pretty] [--omit-nulls] [--omit-empty-records] [--omit-empty-lists]
     [--omit-empty]
```

## Description

The `json` format provides a parser and printer for JSON and [line-delimited
JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON) objects.

### `--schema=<schema>` (Parser)

Provide the name of a [schema](../data-model/schemas.md) used by the parser.

The `--schema` option is incompatible with the `--selector` option.

### `--selector=<field[:prefix]>` (Parser)

Designates a field value as schema name with an optional dot-separated prefix.

For example, the [Suricata EVE JSON](suricata.md) format includes a field
`event_type` that contains the event type. Setting the selector to
`event_type:suricata` causes an event with the value `flow` for the field
`event_type` to map onto the schema `suricata.flow`.

The `--selector` option is incompatible with the `--schema` option.

### `--no-infer` (Parser)

The JSON parser automatically infers types in the input JSON.

The flag `--no-infer` toggles this behavior, and requires the user to provide an
input schema for the JSON to explicitly parse into, e.g., using the `--selector`
option.

Schema inference happens on a best-effort basis, and is constantly being
improved to match Tenzir's type system.

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

### `--ndjson` (Parser)

Treat the input as newline-delimited JSON (NDJSON).

NDJSON requires that exactly one event exists per line. This allows for better
error recovery in cases of malformed input, as unlike for the regular JSON
parser malformed lines can be skipped.

Popular examples of NDJSON include the Suricat Eve JSON and the Zeek Streaming
JSON formats. Tenzir supports [`suricata`][suricata.md] and
[`zeek-json`][zeek-json.md] parsers out of the box that utilize this mechanism.

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
