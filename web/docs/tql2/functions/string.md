# string

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

[int](int.md), [uint](uint.md), [float](float.md), [time](time.md), [ip](ip.md)
