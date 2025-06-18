---
title: is_alnum
category: String/Inspection
example: '"hello123".is_alnum()'
---

Checks if a string is alphanumeric.

```tql
is_alnum(x:string) -> bool
```

## Description

The `is_alnum` function returns `true` if `x` contains only alphanumeric
characters and `false` otherwise.

## Examples

### Check if a string is alphanumeric

```tql
from {x: "hello123".is_alnum()}
```

```tql
{x: true}
```

## See Also

[`is_alpha`](/reference/functions/is_alpha),
[`is_numeric`](/reference/functions/is_numeric),
[`is_printable`](/reference/functions/is_printable)
