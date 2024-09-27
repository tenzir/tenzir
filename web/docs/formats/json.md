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
json [--merge] [--schema <schema>] [--selector <fieldname[:prefix]>]
     [--schema-only] [--raw] [--unnest-separator <separator>]
     [--ndjson] [--arrays-of-objects] [--precise] [--no-infer]
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

### Common Options (Parser)

The JSON parser supports the common [schema inference options](formats.md#parser-schema-inference).

### `--ndjson` (Parser)

Treat the input as newline-delimited JSON (NDJSON).

NDJSON requires that exactly one event exists per line. This allows for better
error recovery in cases of malformed input, as unlike for the regular JSON
parser malformed lines can be skipped.

Popular examples of NDJSON include the Suricata Eve JSON and the Zeek Streaming
JSON formats. Tenzir supports [`suricata`](suricata.md) and
[`zeek-json`](zeek-json.md) parsers out of the box that utilize this mechanism.

### `--precise` (Parser)

Legacy flag. Has the same effect as *not* providing `--merge`. This option is incompatible with  `--merge`.

### `--no-infer` (Parser)

Legacy flag. It is equivalent to the new flag `--schema-only`.

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
