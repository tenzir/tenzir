---
title: trim
category: String/Transformation
example: '" hello ".trim()'
---
Trims whitespace from both ends of a string.

```tql
trim(x:string) -> string
```

## Description

The `trim` function removes leading and trailing whitespace from `x`.

## Examples

### Trim whitespace from both ends

```tql
from {x: " hello ".trim()}
```

```tql
{x: "hello"}
```

## See Also

[`trim_start`](/reference/functions/trim_start),
[`trim_end`](/reference/functions/trim_end)
