# lines

Parses events as lines.

## Synopsis

```
lines [-s|--skip-empty]
```

## Description

The `lines` parser takes its input bytes and splits it at a newline character.

Newline characters include:

- `\n`
- `\r\n`

The resulting events have a single field called `line`.

### `-s|--skip-empty`

Ignores empty lines in the input.

Defaults to `false`.

## Examples

Read a text file line-by-line:

```
from file /tmp/file.txt read lines
```
