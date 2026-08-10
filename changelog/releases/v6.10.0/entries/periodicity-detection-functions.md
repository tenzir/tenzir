---
title: Periodicity detection with `autocorrelation`, `periodogram`, and `dominant_period`
type: feature
authors:
  - mavam
created: 2026-08-06T20:30:00.000000Z
---

Three new functions recover periodic structure from event series, closing the
gap that made research-grade beacon detection impractical in pure TQL:

- `autocorrelation(xs, max_lag=int)` computes normalized
  [autocorrelation](https://en.wikipedia.org/wiki/Autocorrelation)
  coefficients for lags `0` through `max_lag` (default: half the list length).
- `periodogram(xs)` returns spectral power per period via a
  [fast Fourier transform](https://en.wikipedia.org/wiki/Fast_Fourier_transform),
  computing the classical
  [periodogram](https://en.wikipedia.org/wiki/Periodogram).
- `dominant_period(times, resolution=duration)` bins a list of timestamps at
  the given resolution and returns the strongest period together with a
  normalized strength between 0 and 1.

Combined with aggregation, this turns beacon detection into a few lines. The
first destination below checks in every 30 seconds, the second connects at
irregular intervals, and only the beacon survives the strength filter:

```tql
from {dst: 10.0.0.99, t: 2024-01-01T00:00:00},
     {dst: 10.0.0.99, t: 2024-01-01T00:00:30},
     {dst: 10.0.0.99, t: 2024-01-01T00:01:00},
     {dst: 10.0.0.99, t: 2024-01-01T00:01:30},
     {dst: 10.0.0.99, t: 2024-01-01T00:02:00},
     {dst: 10.0.0.99, t: 2024-01-01T00:02:30},
     {dst: 10.0.0.99, t: 2024-01-01T00:03:00},
     {dst: 10.0.0.99, t: 2024-01-01T00:03:30},
     {dst: 172.16.0.5, t: 2024-01-01T00:00:00},
     {dst: 172.16.0.5, t: 2024-01-01T00:00:13},
     {dst: 172.16.0.5, t: 2024-01-01T00:00:47},
     {dst: 172.16.0.5, t: 2024-01-01T00:01:29},
     {dst: 172.16.0.5, t: 2024-01-01T00:02:00},
     {dst: 172.16.0.5, t: 2024-01-01T00:02:21},
     {dst: 172.16.0.5, t: 2024-01-01T00:03:10},
     {dst: 172.16.0.5, t: 2024-01-01T00:03:37}
summarize dst, times=collect(t), samples=count()
beat = times.dominant_period(resolution=5s)
where samples >= 8 and beat.strength >= 0.8
select dst, period=beat.period, strength=round(beat.strength * 100) / 100
```

```tql
{dst: 10.0.0.99, period: 30s, strength: 0.87}
```
