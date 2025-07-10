---
title: pad_right
category: String/Transformation
example: '"hello".pad_right(10)'
---

Pads a string on the right to a specified length.

```tql
pad_right(x:string, length:int, [pad_char:string=" "]) -> string
```

## Description

The `pad_right` function pads the string `x` on the right side with `pad_char` 
(default: space) until it reaches the specified `length`. If the string is 
already longer than or equal to the specified length, it returns the original 
string unchanged.

### `x: string`

The string to pad.

### `length: int`

The target length of the resulting string.

### `pad_char: string = " "`

The character to use for padding. Must be a single character. Defaults to a space.

## Examples

### Pad with spaces

```tql
from {x: "hello".pad_right(10)}
```

```tql
{x: "hello     "}
```

### Pad with custom character

```tql
from {x: "hello".pad_right(10, ".")}
```

```tql
{x: "hello....."}
```

### String already long enough

```tql
from {x: "hello world".pad_right(5)}
```

```tql
{x: "hello world"}
```

## See Also

[`pad_left`](/reference/functions/pad_left),
[`trim`](/reference/functions/trim),
[`trim_end`](/reference/functions/trim_end)