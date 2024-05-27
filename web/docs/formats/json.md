---
sidebar_custom_props:
  format:
    parser: true
    printer: true
---

# json

Reads and writes JSON.

## Synopsis

Parser:

```
json [--schema <schema>] [--selector <field[:prefix]>] [--unnest-separator <string>]
     [--no-infer] [--ndjson] [--precise] [--raw]
     [--arrays-of-objects]
```

Printer:

```
json [-c|--compact-output] [-C|--color-output] [-M|--monochrome-output]
     [--omit-nulls] [--omit-empty-objects] [--omit-empty-lists] [--omit-empty]
     [--arrays-of-objects]
```

## Description

The `json` format provides a parser and printer for JSON and [line-delimited
JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON) objects.

### `--schema=<schema>` (Parser)

Provide the name of a [schema](../data-model/schemas.md) to be used by the
parser. If the schema uses the `blob` type, then the JSON parser expects
base64-encoded strings.

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

Popular examples of NDJSON include the Suricata Eve JSON and the Zeek Streaming
JSON formats. Tenzir supports [`suricata`](suricata.md) and
[`zeek-json`](zeek-json.md) parsers out of the box that utilize this mechanism.

### `--precise` (Parser)

Ensure that only fields that are actually present in the input are contained in
the returned events. Without this option, the input consisting of `{"a": 1}` and
`{"b": 2}` can be result in the events `{"a": 1, "b": null}` and
`{"a": null, "b": 2}`. With it, the output is `{"a": 1}` and `{"b": 2}`. For
some inputs and queries, this can be significantly more expensive.

### `--raw` (Parser)

Use only the raw JSON types. This means that all strings are parsed as `string`,
irrespective of whether they are a valid `ip`, `duration`, etc. Also, since JSON
only has one generic number type, all numbers are parsed with the `double` type.

### `--arrays-of-objects` (Parser)

Parse arrays of objects, with every object in the outermost arrays resulting in
one event each. This is particularly useful when interfacing with REST APIs,
which often yield large arrays of objects instead of newline-delimited JSON
objects.

### `--c|--compact-output` (Printer)

Switch to line-delimited JSON output (JSONL/NDJSON).

### `--C|--color-output` (Printer)

Colorize the output.

The option enables colorizing the output similar to `jq` by emitting terminal
escape sequences that represent colors.

Unlike `jq`, coloring is currently opt-in. In the future, we will perform TTY
detection and colorize the output when write to stdout.

Tenzir honors the [`NO_COLOR`](https://no-color.org/) environment variable and
won't colorize the output when the variable is present.

### `--M|--monochrome-output` (Printer)

Disables colored output.

This is currently the default. In the future, we will perform TTY detection and
colorize the output when write to stdout. Use this option today if you want to
avoid an implicit upgrade to colors in the future.

### `--omit-nulls` (Printer)

Strips `null` fields from the output.

Example:

```json
{
  "a": null,
  "b": [42, null, 43],
  "c": {
    "d": null,
    "e": 42
  }
}
```

With `--omit-nulls`, this example becomes:

```json
{
  "b": [42, 43],
  "c": {
    "e": 42
  }
}
```

### `--omit-empty-objects` (Printer)

Strips empty objects from the output.

Example:

```json
{
  "w": null,
  "x": {},
  "y": {
    "z": {}
  }
}
```

With `--omit-empty-objects`, this example becomes:

```json
{
  "w": 42,
}
```

### `--omit-empty-lists` (Printer)

Strips empty lists from the output.

Example:

```json
{
  "m": [],
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

### `--arrays-of-objects` (Printer)

Prints one array of objects per batch of events arriving at the printer as
opposed to printing one object per event.

This is particularly useful when interfacing with REST APIs, which often require
sets of events grouped into one JSON object.

Use the [`batch`](../operators/batch.md) operator to explicitly control how many
events get grouped together in the same array.

Example:

```
{
  "foo": 1
}
{
  "foo": 2
}
```

With `--arrays-of-objects`, this example becomes:

```
[{
  "foo": 1
},
{
  "foo": 2
}]
```

## Examples

Read JSON from stdin, either NDJSON or tree-structured:

```
read json
```

Write JSON without empty fields to a file:

```
to file /tmp/result.json write json --omit-empty
```

Print NDJSON to stdout:

```
write json -c
```
