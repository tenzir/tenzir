---
title: to_lower
category: String/Transformation
example: '"HELLO".to_lower()'
---

Converts a string to lowercase.

```tql
to_lower(x:string) -> string
```

## Description

The `to_lower` function converts all characters in `x` to lowercase.

## Examples

### Convert a string to lowercase

```tql
from {x: "HELLO".to_lower()}
```

```tql
{x: "hello"}
```

## See Also

[`capitalize`](/reference/functions/capitalize),
[`is_lower`](/reference/functions/is_lower),
[`to_title`](/reference/functions/to_title),
[`to_upper`](/reference/functions/to_upper)
