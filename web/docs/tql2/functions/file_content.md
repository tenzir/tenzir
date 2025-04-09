# file_content

Reads a file's contents.

```tql
file_content(path:string) -> blob
```

## Description

The `file_content` function reads a file's contents.

### `path: string`

Absolute path of file to read.

## Examples

```tql
let $secops_config = file_content("/path/to/file.json").string().parse_json()
…
to_google_secops client_email=$secops_config.client_email, …
```
