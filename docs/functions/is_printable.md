---
title: is_printable
---

Checks if a string contains only printable characters.

```tql
is_printable(x:string) -> bool
```

## Description

The `is_printable` function returns `true` if `x` contains only printable
characters and `false` otherwise.

## Examples

### Check if a string is printable

```tql
from {x: "hello".is_printable()}
```

```tql
{x: true}
```

## See Also

[`is_alnum`](/reference/functions/is_alnum),
[`is_alpha`](/reference/functions/is_alpha)
