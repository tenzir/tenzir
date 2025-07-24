---
title: replace
category: String/Transformation
example: '"hello".replace("o", "a")'
---

Replaces characters within a string.

```tql
replace(x:string, pattern:string, replacement:string, [max=int]) -> string
```

## Description

The `replace` function returns a new string where occurrences of `pattern` in `x`
are replaced with `replacement`, up to `max` times. If `max` is omitted, all
occurrences are replaced.

### `x: string`

The subject to replace the action on.

### `pattern: string`

The pattern to replace in `x`.

### `replacement: string`

The replacement value for `pattern`.

### `max = int (optional)`

The maximum number of replacements to perform.

If the option is not set, all occurrences are replaced.

## Examples

### Replace all occurrences of a character

```tql
from {x: "hello".replace("l", "r")}
```

```tql
{x: "herro"}
```

### Replace a limited number of occurrences

```tql
from {x: "hello".replace("l", "r", max=1)}
```

```tql
{x: "herlo"}
```

## See Also

[`replace_regex`](/reference/functions/replace_regex),
[`replace`](/reference/operators/replace)
