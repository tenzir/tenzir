---
title: save_file
category: Outputs/Bytes
example: 'save_file "/tmp/out.json"'
---
Writes a byte stream to a file.

```tql
save_file path:string, [append=bool, real_time=bool, uds=bool]
```

## Description

Writes a byte stream to a file.

### `path: string`

The file path to write to. If intermediate directories do not exist, they will
be created. When `~` is the first character, it will be substituted with the
value of the `$HOME` environment variable.

### `append = bool (optional)`

If `true`, appends to the file instead of overwriting it.

### `real_time = bool (optional)`

If `true`, immediately synchronizes the file with every chunk of bytes instead
of buffering bytes to batch filesystem write operations.

### `uds = bool (optional)`

If `true`, creates a Unix Domain Socket instead of a normal file. Cannot be
combined with `append=true`.

## Examples

### Save bytes to a file

```tql
save_file "/tmp/out.txt"
```

## See Also

[`files`](/reference/operators/files),
[`load_file`](/reference/operators/load_file),
[`save_stdout`](/reference/operators/save_stdout)
