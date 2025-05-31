---
title: bit_xor
category: Bit Operations
example: 'bit_xor(lhs, rhs)'
---
Computes the bit-wise XOR of its arguments.

```tql
bit_xor(lhs:number, rhs:number) -> number
```

## Description

The `bit_xor` function computes the bit-wise XOR (exclusive OR) of `lhs` and
`rhs`. The operation is performed on each corresponding bit position of the two
numbers.

### `lhs: number`

The left-hand side operand.

### `rhs: number`

The right-hand side operand.

## Examples

### Perform bit-wise XOR on integers

```tql
from {x: bit_xor(5, 3)}
```

```tql
{x: 6}
```

## See Also

[`bit_and`](/reference/functions/bit_and),
[`bit_or`](/reference/functions/bit_or),
[`bit_not`](/reference/functions/bit_not),
[`shift_left`](/reference/functions/shift_left),
[`shift_right`](/reference/functions/shift_right)
