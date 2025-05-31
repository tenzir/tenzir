---
title: capitalize
category: String/Transformation
example: '"hello".capitalize()'
---

Capitalizes the first character of a string.

```tql
capitalize(x:string) -> string
```

## Description

The `capitalize` function returns the input string with the first character
converted to uppercase and the rest to lowercase.

## Examples

### Capitalize a lowercase string

```tql
from {x: "hello world".capitalize()}
```

```tql
{x: "Hello world"}
```

## See Also

[`to_upper`](/reference/functions/to_upper),
[`to_lower`](/reference/functions/to_lower),
[`to_title`](/reference/functions/to_title)
