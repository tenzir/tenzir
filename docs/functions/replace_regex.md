---
title: replace_regex
category: String/Transformation
example: '"hello".replace("l+o", "y")'
---

Replaces characters within a string based on a regular expression.

```tql
replace_regex(x:string, pattern:string, replacement:string, [max=int]) -> string
```

## Description

The `replace_regex` function returns a new string where substrings in `x` that
match `pattern` are replaced with `replacement`, up to `max` times. If `max` is
omitted, all matches are replaced.

### `x: string`

The subject to replace the action on.

### `pattern: string`

The pattern (as regular expression) to replace in `x`.

### `replacement: string`

The replacement value for `pattern`.

### `max = int (optional)`

The maximum number of replacements to perform.

If the option is not set, all occurrences are replaced.

## Examples

### Replace all matches of a regular expression

```tql
from {x: replace_regex("hello", "l+", "y")}
```

```tql
{x: "heyo"}
```

### Replace a limited number of matches

```tql
from {x: replace_regex("hellolo", "l+", "y", max=1)}
```

```tql
{x: "heyolo"}
```

## See Also

[`replace`](/reference/functions/replace)
