---
title: starts_with
category: String/Inspection
example: '"hello".starts_with("he")'
---

Checks if a string starts with a specified substring.

```tql
starts_with(x:string, prefix:string) -> bool
```

## Description

The `starts_with` function returns `true` if `x` starts with `prefix` and
`false` otherwise.

## Examples

### Check if a string starts with a substring

```tql
from {x: "hello".starts_with("he")}
```

```tql
{x: true}
```

## See Also

[`ends_with`](/reference/functions/ends_with)
