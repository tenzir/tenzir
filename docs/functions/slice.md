---
title: slice
---

Slices a string with offsets and strides.

```tql
slice(x:string, [begin=int, end=int, stride=int])
```

## Description

The `slice` function takes a string as input and selects parts from it.

### `x: string`

The string to slice.

### `begin = int (optional)`

The offset to start slice from.

If negative, offset is calculated from the end of string.

Defaults to `0`.

### `end = int (optional)`

The offset to end the slice at.

If negative, offset is calculated from the end of string.

If unspecified, ends at the `input`'s end.

### `stride = int (optional)`

The difference between the current character to take and the next character to
take.

If negative, characters are chosen in reverse.

Defaults to `1`.

## Examples

### Get the first 3 characters of a string

```tql
from {x: "123456789"}
x = x.slice(end=3)
```

```tql
{x: "123"}
```

### Get the 1st, 3rd, and 5th characters

```tql
from {x: "1234567890"}
x = x.slice(stride=2, end=6)
```

```tql
{x: "135"}
```

### Select a substring from the 2nd character up to the 8th character

```tql
from {x: "1234567890"}
x = x.slice(begin=1, end=8)
```

```tql
{x: "2345678"}
```
