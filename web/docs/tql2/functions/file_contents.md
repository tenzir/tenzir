# file_contents

Reads a file's contents.

```tql
file_contents(path:string) -> blob
```

## Description

The `file_contents` function reads a file's contents.

### `path: string`

Absolute path of file to read.

## Examples

```tql
let $secops_config = file_contents("/path/to/file.json").string().parse_json()
…
to_google_secops client_email=$secops_config.client_email, …
```
