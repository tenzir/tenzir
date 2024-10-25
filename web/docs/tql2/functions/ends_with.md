# ends_with

Checks if a string ends with a specified substring.

```tql
ends_with(x:string, suffix:string) -> bool
```

## Description

The `ends_with` function returns `true` if `x` ends with `suffix` and `false`
otherwise.

## Examples

### Check if a string ends with a substring

```tql
from {x: ends_with("hello", "lo")}
```

```tql
{x: true}
```

## See Also

[`starts_with`](starts_with.md)
