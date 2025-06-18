---
title: trim_end
category: String/Transformation
example: '"hello ".trim_end()'
---

Trims whitespace from the end of a string.

```tql
trim_end(x:string) -> string
```

## Description

The `trim_end` function removes trailing whitespace from `x`.

## Examples

### Trim whitespace from the end

```tql
from {x: "hello ".trim_end()}
```

```tql
{x: "hello"}
```

## See Also

[`trim`](/reference/functions/trim),
[`trim_start`](/reference/functions/trim_start)
