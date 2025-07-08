---
title: subnet
category: Type System/Conversion
example: 'subnet("1.2.3.4/16")'
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
[`is_v4`](/reference/functions/is_v4),
[`is_v6`](/reference/functions/is_v6),
[`is_multicast`](/reference/functions/is_multicast),
[`is_loopback`](/reference/functions/is_loopback),
[`is_private`](/reference/functions/is_private),
[`is_global`](/reference/functions/is_global),
[`is_link_local`](/reference/functions/is_link_local),
[`ip_type`](/reference/functions/ip_type),
[`time`](/reference/functions/time),
[`uint`](/reference/functions/uint),
[float](/reference/functions/float),
[string](/reference/functions/string)
