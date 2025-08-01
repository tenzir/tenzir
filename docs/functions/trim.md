---
title: trim
category: String/Transformation
example: '" hello ".trim()'
---

Trims whitespace or specified characters from both ends of a string.

```tql
trim(x:string, [chars:string]) -> string
```

## Description

The `trim` function removes characters from both ends of `x`.

When called with one argument, it removes leading and trailing whitespace.
When called with two arguments, it removes any characters found in `chars` from
both ends of the string.

### `x: string`

The string to trim.

### `chars: string (optional)`

A string where each character represents a character to remove. Any character
found in this string will be trimmed from both ends.

Defaults to whitespace characters.

## Examples

### Trim whitespace from both ends

```tql
from {x: " hello ".trim()}
```

```tql
{x: "hello"}
```

### Trim specific characters

```tql
from {x: "/path/to/file/".trim("/")}
```

```tql
{x: "path/to/file"}
```

### Trim multiple characters

```tql
from {x: "--hello--world--".trim("-")}
```

```tql
{x: "hello--world"}
```

## See Also

[`trim_start`](/reference/functions/trim_start),
[`trim_end`](/reference/functions/trim_end)
