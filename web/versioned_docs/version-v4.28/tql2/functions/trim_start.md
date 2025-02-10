# trim_start

Trims whitespace from the start of a string.

```tql
trim_start(x:string) -> string
```

## Description

The `trim_start` function removes leading whitespace from `x`.

## Examples

### Trim whitespace from the start

```tql
from {x: " hello".trim_start()}
```

```tql
{x: "hello"}
```

## See Also

[`trim`](trim.md), [`trim_end`](trim_end.md)
