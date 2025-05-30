---
title: subnet
---

Casts an expression to a subnet value.

```tql
subnet(x:string) -> subnet
```

## Description

The `subnet` function casts an expression to a subnet.

### `x: string`

The string expression to cast.

## Examples

### Cast a string to a subnet

```tql
from {x: subnet("1.2.3.4/16")}
```

```tql
{x: 1.2.0.0/16}
```

## See Also

[`int`](/reference/functions/int),
[`ip`](/reference/functions/ip),
[`time`](/reference/functions/time),
[`uint`](/reference/functions/uint),
[float](/reference/functions/float),
[string](/reference/functions/string)
