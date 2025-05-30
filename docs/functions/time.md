---
title: time
---

Casts an expression to a time value.

```tql
time(x:any) -> time
```

## Description

The `time` function casts the given string or number `x` to a time value.

## Examples

### Cast a string to a time value

```tql
from {x: time("2020-03-15")}
```

```tql
{x: 2020-03-15T00:00:00.000000}
```

## See Also

[`ip`](/reference/functions/ip),
[`string`](/reference/functions/string),
[`subnet`](/reference/functions/subnet),
[`uint`](/reference/functions/uint),
[duration](/reference/functions/duration),
[float](/reference/functions/float),
[int](/reference/functions/int)
