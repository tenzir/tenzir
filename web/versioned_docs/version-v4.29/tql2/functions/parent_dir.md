# parent_dir

Extracts the parent directory from a file path.

```tql
parent_dir(x:string) -> string
```

## Description

The `parent_dir` function returns the parent directory path of the given file path, excluding the file name.

## Examples

### Extract the parent directory from a file path

```tql
from {x: parent_dir("/path/to/log.json")}
```

```tql
{x: "/path/to"}
```

## See Also

[`file_name`](file_name.md)
