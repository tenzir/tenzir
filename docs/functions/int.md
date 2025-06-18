---
title: int
category: Type System/Conversion
example: 'int(-4.2)'
---

Casts an expression to an integer.

```tql
int(x:number|string, base=int) -> int
```

## Description

The `int` function casts the provided value `x` to an integer. Non-integer
values are truncated.

### `x: number|string`

The input to convert.

### `base = int`

Base (radix) to parse a string as. Can be `10` or `16`.

If `16`, the string inputs may be optionally prefixed by `0x` or `0X`, e.g.,
`-0x134`.

Defaults to `10`.

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

### Parse a hexadecimal number

```tql
from {x: int("0x42", base=16)}
```

```tql
{x: 66}
```

## See Also

[`ip`](/reference/functions/ip),
[`subnet`](/reference/functions/subnet),
[`time`](/reference/functions/time),
[`uint`](/reference/functions/uint),
[float](/reference/functions/float),
[string](/reference/functions/string)
