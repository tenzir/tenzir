---
title: is_lower
category: String/Inspection
example: '"hello".is_lower()'
---

Checks if a string is in lowercase.

```tql
is_lower(x:string) -> bool
```

## Description

The `is_lower` function returns `true` if `x` is entirely in lowercase and
`false` otherwise.

## Examples

### Check if a string is lowercase

```tql
from {x: "hello".is_lower()}
```

```tql
{x: true}
```

## See Also

[`is_alpha`](/reference/functions/is_alpha),
[`is_upper`](/reference/functions/is_upper),
[`to_lower`](/reference/functions/to_lower)
