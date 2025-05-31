---
title: split
category: String/Transformation
example: 'split("a,b,c", ",")'
---
Splits a string into substrings.

```tql
split(x:string, pattern:string, [max:int], [reverse:bool]) -> list
```

## Description

The `split` function splits the input string `x` into a list of substrings
using the specified `pattern`. Optional arguments allow limiting the number
of splits (`max`) and reversing the splitting direction (`reverse`).

### `x: string`

The string to split.

### `pattern: string`

The delimiter or pattern used for splitting.

### `max: int (optional)`

The maximum number of splits to perform.

Defaults to `0`, meaning no limit.

### `reverse: bool (optional)`

If `true`, splits from the end of the string.

Defaults to `false`.

## Examples

### Split a string by a delimiter

```tql
from {xs: split("a,b,c", ",")}
```

```tql
{xs: ["a", "b", "c"]}
```

### Limit the number of splits

```tql
from {xs: split("a-b-c", "-", max=1)}
```

```tql
{xs: ["a", "b-c"]}
```

## See Also

[`split_regex`](/reference/functions/split_regex),
[`join`](/reference/functions/join)
