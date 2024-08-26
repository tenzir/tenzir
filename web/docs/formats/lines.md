---
sidebar_custom_props:
  format:
    parser: true
    printer: true
---

# lines

Parses and prints events as lines.

## Synopsis

Parser:

```
lines [-s|--skip-empty]
```

Printer:

```
lines
```

## Description

The `lines` parser takes its input bytes and splits it at a newline character.

Newline characters include:

- `\n`
- `\r\n`

The resulting events have a single field called `line`.

Each event is printed on a new line, with fields separated by spaces,
and nulls skipped.
Use the `put`-operator before the `lines` printer to only print a single field.

:::info
The lines printer does not perform any escaping. Characters like `\n` and `"` are printed
as-is.
:::

### `-s|--skip-empty` (Parser)

Ignores empty lines in the input.

Defaults to `false`.

## Examples

Read a text file line-by-line:

```
from file /tmp/file.txt read lines
```

Write the version number to stdout:

```
version | put version | to - write lines
```
