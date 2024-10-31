# replace

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

### `max = string` (optional)

The maximum number of replacements to perform.

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

[`replace_regex`](replace_regex.md)
