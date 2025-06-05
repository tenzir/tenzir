---
title: ends_with
category: String/Inspection
example: '"hello".ends_with("lo")'
---

Checks if a string ends with a specified substring.

```tql
ends_with(x:string, suffix:string) -> bool
```

## Description

The `ends_with` function returns `true` if `x` ends with `suffix` and `false`
otherwise.

## Examples

### Check if a string ends with a substring

```tql
from {x: "hello".ends_with("lo")}
```

```tql
{x: true}
```

## See Also

[`starts_with`](/reference/functions/starts_with)
