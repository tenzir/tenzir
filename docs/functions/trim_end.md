---
title: trim_end
category: String/Transformation
example: '"hello ".trim_end()'
---

Trims whitespace or specified characters from the end of a string.

```tql
trim_end(x:string, [chars:string]) -> string
```

## Description

The `trim_end` function removes characters from the end of `x`.

When called with one argument, it removes trailing whitespace.
When called with two arguments, it removes any characters found in `chars` from
the end of the string.

### `x: string`

The string to trim.

### `chars: string` (optional)

The characters to remove.

Defaults to whitespace characters.

## Examples

### Trim whitespace from the end

```tql
from {x: "hello ".trim_end()}
```

```tql
{x: "hello"}
```

### Trim specific characters

```tql
from {x: "/path/to/file/".trim_end("/")}
```

```tql
{x: "/path/to/file"}
```

### Trim multiple characters

```tql
from {x: "hello/-/".trim_end("/-")}
```

```tql
{x: "hello"}
```

## See Also

[`trim`](/reference/functions/trim),
[`trim_start`](/reference/functions/trim_start)
