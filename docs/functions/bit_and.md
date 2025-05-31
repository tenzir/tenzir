---
title: bit_and
category: Bit Operations
example: 'bit_and(lhs, rhs)'
---

Computes the bit-wise AND of its arguments.

```tql
bit_and(lhs:number, rhs:number) -> number
```

## Description

The `bit_and` function computes the bit-wise AND of `lhs` and `rhs`. The
operation is performed on each corresponding bit position of the two numbers.

### `lhs: number`

The left-hand side operand.

### `rhs: number`

The right-hand side operand.

## Examples

### Perform bit-wise AND on integers

```tql
from {x: bit_and(5, 3)}
```

```tql
{x: 1}
```

## See Also

[`bit_or`](/reference/functions/bit_or),
[`bit_xor`](/reference/functions/bit_xor),
[`bit_not`](/reference/functions/bit_not),
[`shift_left`](/reference/functions/shift_left),
[`shift_right`](/reference/functions/shift_right)
