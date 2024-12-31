# double

Casts an expression to a double.

```tql
double(x:any) -> double
```

## Description

The `double` function converts the given value `x` to a double value.

## Examples

### Cast an integer to a double

```tql
from {x: double(42)}
```

```tql
{x: 42.0}
```

### Cast a string to a double

```tql
from {x: double("4.2")}
```

```tql
{x: 4.2}
```

## See Also

[int](int.md), [uint](uint.md), [time](time.md), [string](string.md), [ip](ip.md)
