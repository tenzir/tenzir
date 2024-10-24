# sqrt

Computes the square root of a number.

```tql
sqrt(x:number) -> float
```

## Description

The `sqrt` function computes the [square
root](https://en.wikipedia.org/wiki/Square_root) of any non-negative number `x`.

## Examples

### Compute the square root of an integer

```tql
from {x: sqrt(49)}
```

```tql
{x: 7.0}
```

### Fail to compute the square root of a negative number

```tql
from {x: sqrt(-1)}
```

```tql
{x: null}
```
