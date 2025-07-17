---
title: pad_start
category: String/Transformation
example: '"hello".pad_start(10)'
---

Pads a string at the start to a specified length.

```tql
pad_start(x:string, length:int, [pad_char:string]) -> string
```

## Description

The `pad_start` function pads the string `x` at the start with `pad_char`
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
from {x: "hello".pad_start(10)}
```

```tql
{x: "     hello"}
```

### Pad with custom character

```tql
from {x: "42".pad_start(5, "0")}
```

```tql
{x: "00042"}
```

### String already long enough

```tql
from {x: "hello world".pad_start(5)}
```

```tql
{x: "hello world"}
```

## See Also

[`pad_end`](/reference/functions/pad_end),
[`trim`](/reference/functions/trim),
[`trim_start`](/reference/functions/trim_start)
