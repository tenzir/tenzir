---
title: abs
---

Returns the absolute value.

```tql
abs(x:number) -> number
abs(x:duration) -> duration
```

## Description

This function returns the [absolute
value](https://en.wikipedia.org/wiki/Absolute_value) for a number or a duration.

### `x: duration|number`

The value to compute absolute value for.

## Examples

```tql
from {x: -13.3}
x = x.abs()
```

```tql
{x: 13.3}
```
