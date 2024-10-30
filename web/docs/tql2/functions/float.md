# float

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

[int](int.md), [uint](uint.md), [time](time.md), [str](str.md), [ip](ip.md)
