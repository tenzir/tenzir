---
title: duration
---

Casts an expression to a duration value.

```tql
duration(x:string) -> duration
```

## Description

The `duration` function casts the given string `x` to a duration value.

## Examples

### Cast a string to a duration

```tql
from {str: "1ms"}
dur = duration(str)
```

```tql
{str: "1ms", dur: 1ms}
```

## See Also

[time](/reference/functions/time)
