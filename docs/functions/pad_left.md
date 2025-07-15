---
title: pad_left
category: String/Transformation
example: '"hello".pad_left(10)'
---

Pads a string on the left to a specified length.

```tql
pad_left(x:string, length:int, [pad_char:string]) -> string
```

## Description

The `pad_left` function pads the string `x` on the left side with `pad_char`
(default: space) until it reaches the specified `length`. If the string is
already longer than or equal to the specified length, it returns the original
string unchanged.

### `x: string`

The string to pad.

### `length: int`

The target length of the resulting string.

### `pad_char: string`

The character to use for padding. Must be a single character. Defaults to a space.

Defaults to `" "`.

## Examples

### Pad with spaces

```tql
from {x: "hello".pad_left(10)}
```

```tql
{x: "     hello"}
```

### Pad with custom character

```tql
from {x: "42".pad_left(5, "0")}
```

```tql
{x: "00042"}
```

### String already long enough

```tql
from {x: "hello world".pad_left(5)}
```

```tql
{x: "hello world"}
```

## See Also

[`pad_right`](/reference/functions/pad_right),
[`trim`](/reference/functions/trim),
[`trim_start`](/reference/functions/trim_start)
