---
title: save_stdout
category: Outputs/Bytes
example: 'save_stdout'
---
Writes a byte stream to standard output.

```tql
save_stdout
```

## Description

Writes a byte stream to standard output. This is mostly useful when using the
`tenzir` executable as part of a shell script.

## Examples

### Write colored, compact TQL-style

```tql
from {x: "Hello World"}
write_tql compact=true, color=true
save_stdout
```

```tql
{x: "Hello World"}
```

## See Also

[`load_stdin`](/reference/operators/load_stdin),
[`save_file`](/reference/operators/save_file)
