---
title: is_numeric
category: String/Inspection
example: '"1234".is_numeric()'
---

Checks if a string contains only numeric characters.

```tql
is_numeric(x:string) -> bool
```

## Description

The `is_numeric` function returns `true` if `x` contains only numeric characters
and `false` otherwise.

## Examples

### Check if a string is numeric

```tql
from {x: "1234".is_numeric()}
```

```tql
{x: true}
```

## See Also

[`is_alpha`](/reference/functions/is_alpha),
[`is_alnum`](/reference/functions/is_alnum)
