---
title: split_regex
---

Splits a string into substrings with a regex.

```tql
split_regex(x:string, pattern:string, [max:int], [reverse:bool]) -> list
```

## Description

The `split_regex` function splits the input string `x` into a list of
substrings using the specified regular expression `pattern`. Optional
arguments allow limiting the number of splits (`max`) and reversing the
splitting direction (`reverse`).

### `x: string`

The string to split.

### `pattern: string`

The regular expression used for splitting.

### `max: int (optional)`

The maximum number of splits to perform.

Defaults to `0`, meaning no limit.

### `reverse: bool (optional)`

If `true`, splits from the end of the string.

Defaults to `false`.

## Examples

### Split a string using a regex pattern

```tql
from {xs: split_regex("a1b2c", r"\d")}
```

```tql
{xs: ["a", "b", "c", ""]}
```

### Limit the number of splits

```tql
from {xs: split_regex("a1b2c3", r"\d", max=1)}
```

```tql
{xs: ["a", "b2c3"]}
```

## See Also

[`split`](/reference/functions/split),
[`join`](/reference/functions/join)
