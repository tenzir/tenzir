# file_name

Extracts the file name from a file path.

```tql
file_name(x:string) -> string
```

## Description

The `file_name` function returns the file name component of a file path,
excluding the parent directories.

## Examples

### Extract the file name from a file path

```tql
from {x: file_name("/path/to/log.json")}
```

```tql
{x: "log.json"}
```

## See Also

[`parent_dir`](parent_dir.md)
