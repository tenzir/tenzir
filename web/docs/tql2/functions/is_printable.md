# is_printable

Checks if a string contains only printable characters.

```tql
is_printable(x:string) -> bool
```

## Description

The `is_printable` function returns `true` if `x` contains only printable
characters and `false` otherwise.

## Examples

### Check if a string is printable

```tql
from {x: is_printable("hello")}
```

```tql
{x: true}
```

## See Also

[`is_alnum`](is_alnum.md), [`is_alpha`](is_alpha.md)
