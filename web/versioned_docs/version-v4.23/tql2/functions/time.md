# time

Casts an expression to a time value.

```tql
time(x:any) -> time
```

## Description

The `time` function casts the given string or number `x` to a time value.

## Examples

### Cast a string to a time value

```tql
from {x: time("2020-03-15")}
```

```tql
{x: 2020-03-15T00:00:00.000000}
```

## See Also

[int](int.md), [uint](uint.md), [float](float.md), [str](str.md), [ip](ip.md)
