# subnet

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

[ip](ip.md), [int](int.md), [uint](uint.md), [float](float.md), [time](time.md), [string](string.md)
