---
title: file_contents
category: String/Filesystem
example: 'file_contents("/path/to/file")'
---

Reads a file's contents.

```tql
file_contents(path:string, [binary=bool]) -> blob|string
```

## Description

The `file_contents` function reads a file's contents.

### `path: string`

Absolute path of file to read.

### `binary = bool (optional)`

Whether to read the file contents as a `blob`, instead of a `string`.

Defaults to `false`.

## Examples

```tql
let $secops_config = file_contents("/path/to/file.json").parse_json()
…
to_google_secops client_email=$secops_config.client_email, …
```
