---
title: string
category: Type System/Conversion
example: 'string(1.2.3.4)'
---

Casts an expression to a string.

```tql
string(x:any) -> string
```

## Description

The `string` function casts the given value `x` to a string.

## Examples

### Cast an IP address to a string

```tql
from {x: string(1.2.3.4)}
```

```tql
{x: "1.2.3.4"}
```

## See Also

[`ip`](/reference/functions/ip),
[`subnet`](/reference/functions/subnet),
[`time`](/reference/functions/time),
[`uint`](/reference/functions/uint),
[float](/reference/functions/float),
[int](/reference/functions/int)
