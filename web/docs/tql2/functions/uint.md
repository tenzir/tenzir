# uint

Casts an expression to an unsigned integer.

```tql
uint(x:any) -> uint
```

## Description

The `uint` function casts the provided value `x` to an unsigned integer.
Non-integer values are truncated.

## Examples

### Cast a floating-point number to an unsigned integer

```tql
from {x: uint(4.2)}
```

```tql
{x: 4}
```
