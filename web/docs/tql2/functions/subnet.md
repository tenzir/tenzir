# subnet

Casts an expression to a subnet mask.

```tql
subnet(x:string) -> subnet
```

## Description

The `sub` function casts the provided string `x` to a subnet

## Examples

### Cast a string to an IP address

```tql
from {x: subnet("1.2.3.4/16")}
```

```tql
{x: 1.2.0.0/16}
```

## See Also

[ip](ip.md), [int](int.md), [uint](uint.md), [float](float.md), [time](time.md), [string](string.md)
