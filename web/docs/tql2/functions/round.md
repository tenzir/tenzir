# round

Rounds a number or a time/duration with a specified unit.

```tql
round(x:number)
round(x:time, unit:duration)
round(x:duration, unit:duration)
```

## Description

The `round` function rounds a number `x` to an integer.

For time and duration values, use the second `unit` argument to define the
rounding unit.

## Examples

### Round integers

```tql
from {
  x: round(3.4),
  y: round(3.5),
  z: round(-3.4),
}
```

```tql
{
  x: 3,
  y: 4,
  z: -3,
}
```

### Round time and duration values

```tql
from {
  x: round(2024-08-23, 1y)
  y: round(42m, 1h)
}
```

```tql
{
  x: 2025-01-01,
  y: 1h,
}
```

## See Also

[`ceil`](ceil.md), [`floor`](floor.md)
