 # int

Casts an expression to an integer.

```tql
int(x:any) -> int
```

## Description

The `int` function casts the provided value `x` to an integer. Non-integer
values are truncated.

## Examples

### Cast a floating-point number to an integer

```tql
from {x: int(4.2)}
```

```tql
{x: 4}
```

### Convert a string to an integer

```tql
from {x: int("42")}
```

```tql
{x: 42}
```

## See Also

[uint](uint.md), [float](float.md), [time](time.md), [str](str.md), [ip](ip.md)
