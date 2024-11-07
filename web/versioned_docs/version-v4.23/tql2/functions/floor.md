# floor

Computes the floor of a number or a time/duration with a specified unit.

```tql
floor(x:number)
floor(x:time, unit:duration)
floor(x:duration, unit:duration)
```

## Description

The `floor` function takes the
[floor](https://en.wikipedia.org/wiki/Floor_and_ceiling_functions) of a number
`x`.

For time and duration values, use the second `unit` argument to define the
rounding unit.

## Examples

### Take the floor of integers

```tql
from {
  x: floor(3.4),
  y: floor(3.5),
  z: floor(-3.4),
}
```

```tql
{
  x: 3,
  y: 3,
  z: -4,
}
```

### Round time and duration values down to a unit

```tql
from {
  x: floor(2024-02-24, 1y),
  y: floor(1h52m, 1h)
}
```

```tql
{
  x: 2024-01-01,
  y: 1h,
}
```

## See Also

[`ceil`](ceil.md), [`round`](round.md)
