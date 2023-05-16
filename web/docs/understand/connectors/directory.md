# directory

Saves bytes to one file per schema into a directory.

## Synopsis

```
directory <path>
```

## Description

The `directory` saver writes one file per schema into the provided directory.

The default printer for the `directory` saver is [`json`](../formats/json.md).

### `<path>`

The path to the directory. If `<path>` does not point to an existing directory,
the saver creates a new directory, including potential intermediate directories.

## Examples

Write one JSON file per unique schema to `/tmp/dir`:

```
write json to directory /tmp/dir
```
