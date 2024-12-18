# ip

Casts an expression to an IP address.

```tql
ip(x:string) -> ip
```

## Description

The `ip` function casts the provided string `x` to an IP address.

## Examples

### Cast a string to an IP address

```tql
from {x: ip("1.2.3.4")}
```

```tql
{x: 1.2.3.4}
```

## See Also

[subnet](subnet.md), [int](int.md), [uint](uint.md), [float](float.md), [time](time.md), [string](string.md)
