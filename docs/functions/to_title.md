---
title: to_title
---

Converts a string to title case.

```tql
to_title(x:string) -> string
```

## Description

The `to_title` function converts all words in `x` to title case.

## Examples

### Convert a string to title case

```tql
from {x: "hello world".to_title()}
```

```tql
{x: "Hello World"}
```

## See Also

[`capitalize`](/reference/functions/capitalize),
[`is_title`](/reference/functions/is_title),
[`to_lower`](/reference/functions/to_lower),
[`to_upper`](/reference/functions/to_upper)
