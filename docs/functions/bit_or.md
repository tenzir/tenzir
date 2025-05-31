---
title: bit_or
category: Bit Operations
example: 'bit_or(lhs, rhs)'
---
Computes the bit-wise OR of its arguments.

```tql
bit_or(lhs:number, rhs:number) -> number
```

## Description

The `bit_or` function computes the bit-wise OR of `lhs` and `rhs`. The operation
is performed on each corresponding bit position of the two numbers.

### `lhs: number`

The left-hand side operand.

### `rhs: number`

The right-hand side operand.

## Examples

### Perform bit-wise OR on integers

```tql
from {x: bit_or(5, 3)}
```

```tql
{x: 7}
```

## See Also

[`bit_and`](/reference/functions/bit_and),
[`bit_xor`](/reference/functions/bit_xor),
[`bit_not`](/reference/functions/bit_not),
[`shift_left`](/reference/functions/shift_left),
[`shift_right`](/reference/functions/shift_right)
