---
sidebar_custom_props:
  connector:
    saver: true
---

# directory

Saves bytes to one file per schema into a directory.

## Synopsis

```
directory [-a|--append] [-r|--real-time] <path>
```

## Description

The `directory` saver writes one file per schema into the provided directory.

The default printer for the `directory` saver is [`json`](../formats/json.md).

### `-a|--append`

Append to files in `path` instead of overwriting them with a new file.

### `-r|--real-time`

Immediately synchronize files in `path` with every chunk of bytes instead of
buffering bytes to batch filesystem write operations.

### `<path>`

The path to the directory. If `<path>` does not point to an existing directory,
the saver creates a new directory, including potential intermediate directories.

## Examples

Write one JSON file per unique schema to `/tmp/dir`:

```
to directory /tmp/dir write json
```
