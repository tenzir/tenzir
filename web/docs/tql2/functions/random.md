# random

Generates a random number in *[0,1]*.

```tql
random() -> float
```

## Description

The `random` function generates a random number by drawing from a [uniform
distribution](https://en.wikipedia.org/wiki/Continuous_uniform_distribution) in
the interval *[0,1]*.

## Examples

### Generate a random number

```tql
from {x: random()}
```

```tql
{"x": 0.19634716885782455}
```
