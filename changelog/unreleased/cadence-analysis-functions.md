---
title: Cadence analysis functions `deltas`, `mad`, and `skewness`
type: feature
authors:
  - mavam
  - claude
created: 2026-08-06T20:29:01.061321Z
---

Three new functions make robust cadence analytics, such as beacon detection,
natural to express in TQL:

- `deltas` computes the successive differences of a list, turning `n` sorted
  timestamps into `n - 1` inter-arrival intervals in a single call. On
  timestamps it yields durations, which the statistics functions keep typed.
- `mad` computes the
  [median absolute deviation](https://en.wikipedia.org/wiki/Median_absolute_deviation)
  about the median, both as an aggregation function in `summarize` and as a
  list method. Unlike the standard deviation, it shrugs off outliers: a
  single 10-minute gap in an otherwise steady 60-second beacon leaves the MAD
  untouched. Duration input yields a duration result.
- `skewness` computes moment
  [skewness](https://en.wikipedia.org/wiki/Skewness), with
  [quantile-based Bowley skewness](https://en.wikipedia.org/wiki/Skewness#Quantile-based_measures)
  available via `method="bowley"`. Zero-dispersion input, such as a perfectly
  regular beacon, returns `0.0` instead of hitting the 0/0 that hand-rolled
  versions stumble over.

Together they collapse the core of the [RITA](https://github.com/activecm/rita)
beacon scoring model to a few readable lines. Here a beacon checks in
roughly every minute with a few seconds of jitter and misses one check-in
entirely, yet the MAD stays at 3 seconds and the skewness stays bounded:

```tql
from {times: [
  2024-01-01T00:00:00, 2024-01-01T00:00:57, 2024-01-01T00:01:57,
  2024-01-01T00:02:56, 2024-01-01T00:03:59, 2024-01-01T00:05:59,
]}
intervals = times.sort().deltas()
interval_mad = intervals.mad()
interval_skew = round(intervals.skewness(method="bowley") * 10000) / 10000
drop times
```

```tql
{
  intervals: [57s, 1min, 59s, 1.05min, 2min],
  interval_mad: 3s,
  interval_skew: 0.5,
}
```
