---
title: Exponential, logarithmic, and power functions
type: feature
authors:
  - mavam
created: 2026-08-13T09:56:53.554544Z
---

TQL now provides `exp`, `log`, and `pow` for exponential and logarithmic calculations. `log` uses the natural base by default and accepts an optional base for arbitrary logarithms:

```tql
from {
  natural: exp(1).log(),
  binary: log(8, 2),
  power: pow(2, 10),
}
```

```tql
{
  natural: 1.0,
  binary: 3.0,
  power: 1024,
}
```
