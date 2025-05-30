---
title: to_upper
---

Converts a string to uppercase.

```tql
to_upper(x:string) -> string
```

## Description

The `to_upper` function converts all characters in `x` to uppercase.

## Examples

### Convert a string to uppercase

```tql
from {x: "hello".to_upper()}
```

```tql
{x: "HELLO"}
```

## See Also

[`capitalize`](/reference/functions/capitalize),
[`is_upper`](/reference/functions/is_upper),
[`to_lower`](/reference/functions/to_lower),
[`to_title`](/reference/functions/to_title)
