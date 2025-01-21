# reverse

Reverses the characters of a string.

```tql
reverse(x:string) -> string
```

## Description

The `reverse` function returns a new string with the characters of `x` in reverse order.

This function operates on Unicode codepoints, not grapheme clusters. Hence, it
will not correctly reverse grapheme clusters composed of multiple codepoints.

## Examples

### Reverse a string

```tql
from {x: reverse("hello")}
```

```tql
{x: "olleh"}
```
