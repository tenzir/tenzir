---
title: float
category: Type System/Conversion
example: 'float(42)'
---

Casts an expression to a float.

```tql
float(x:any) -> float
```

## Description

The `float` function converts the given value `x` to a floating-point value.

## Examples

### Cast an integer to a float

```tql
from {x: float(42)}
```

```tql
{x: 42.0}
```

### Cast a string to a float

```tql
from {x: float("4.2")}
```

```tql
{x: 4.2}
```

## See Also

[`ip`](/reference/functions/ip),
[`string`](/reference/functions/string),
[`subnet`](/reference/functions/subnet),
[`time`](/reference/functions/time),
[`uint`](/reference/functions/uint),
[int](/reference/functions/int)
