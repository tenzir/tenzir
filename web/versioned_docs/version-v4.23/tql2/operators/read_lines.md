# read_lines

Parses an incoming bytes stream into events.

```tql
read_lines [skip_empty=bool, split_at_null=bool]
```

## Description

The `read_lines` operator takes its input bytes and splits it at a newline character.

Newline characters include:

- `\n`
- `\r\n`

The resulting events have a single field called `line`.

### `skip_empty = bool (optional)`

Ignores empty lines in the input.

### `split_at_null = bool (optional)`

Use null byte (`\0`) as the delimiter instead of newline characters.

## Examples

### Reads lines from a file

```tql
load_file "events.log"
read_lines
is_error = line.starts_with("error:")
```
