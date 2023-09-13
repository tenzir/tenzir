# lines

Parses events as lines.

## Synopsis

```
lines [-f|--field-name <string>] [-s|--skip-empty]
```

## Description

The `lines` parser takes its input bytes and splits it at a newline character.

Newline characters include:

- `\n`
- `\r\n`

### `-f|--field-name <string>`

Provides a different field name for the output.

Defaults to `data`.

### `-s|--skip-empty`

Ignores empty lines in the input.

Defaults to `false`.

## Examples

Read a text file line-by-line:

```
from file /tmp/file.txt read lines
```
