---
title: Exponentially weighted moving averages
type: feature
authors:
  - mavam
  - codex
  - claude
created: 2026-08-08T07:13:23.782028Z
---

TQL now supports exponentially weighted moving averages for numeric lists.
Choose decay by smoothing factor, span, center of mass, or half-life, and
control adjusted weighting and null handling:

```tql
from {xs: [1, null, 3, 4]}
select result = xs.ewma(span=3, ignore_nulls=true)
```

```tql
{result: [1.0, 1.0, 2.3333333333333335, 3.2857142857142856]}
```

Use a duration half-life with a matching timestamp list to account for
irregular sampling intervals:

```tql
from {
  xs: [1.0, 2.0, 3.0],
  times: [2024-01-01T00:00:00, 2024-01-01T02:00:00, 2024-01-01T02:30:00],
}
select result = ewma(xs, halflife=1h, times=times)
```

```tql
{result: [1.0, 1.8, 2.436982071863674]}
```

The companion functions `ewm_variance` and `ewm_stddev` compute the
exponentially weighted variance and standard deviation with the same options,
plus a `bias` option that switches from the debiased to the biased weighted
moment. Together with `ewma`, they turn a smoothed series into a streaming
baseline with a built-in [z-score](https://en.wikipedia.org/wiki/Standard_score),
measuring the deviation from the exponentially weighted mean in units of the
exponentially weighted standard deviation:

```tql
from {xs: [10, 12, 11, 13, 12, 11, 12, 40]}
baseline = xs.ewma(span=20).last()
select zscore = (xs.last() - baseline) / xs.ewm_stddev(span=20).last()
```

```tql
{zscore: 2.0333636871546847}
```
