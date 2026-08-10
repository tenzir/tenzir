This release brings research-grade beacon and anomaly detection into TQL with cadence analysis, periodicity detection, and exponentially weighted statistics. It also improves automatic NetFlow ingestion and ClickHouse JSON interoperability.

## 🚀 Features

### Cadence analysis functions `deltas`, `mad`, and `skewness`

Three new functions make robust cadence analytics, such as beacon detection, natural to express in TQL:

- `deltas` computes the successive differences of a list, turning `n` sorted timestamps into `n - 1` inter-arrival intervals in a single call. On timestamps it yields durations, which the statistics functions keep typed.
- `mad` computes the [median absolute deviation](https://en.wikipedia.org/wiki/Median_absolute_deviation) about the median, both as an aggregation function in `summarize` and as a list method. Unlike the standard deviation, it shrugs off outliers: a single 10-minute gap in an otherwise steady 60-second beacon leaves the MAD untouched. Duration input yields a duration result.
- `skewness` computes moment [skewness](https://en.wikipedia.org/wiki/Skewness), with [quantile-based Bowley skewness](https://en.wikipedia.org/wiki/Skewness#Quantile-based_measures) available via `method="bowley"`. Zero-dispersion input, such as a perfectly regular beacon, returns `0.0` instead of hitting the 0/0 that hand-rolled versions stumble over.

Together they collapse the core of the [RITA](https://github.com/activecm/rita) beacon scoring model to a few readable lines. Here a beacon checks in roughly every minute with a few seconds of jitter and misses one check-in entirely, yet the MAD stays at 3 seconds and the skewness stays bounded:

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

*By @mavam and @claude.*

### Exponentially weighted moving averages

TQL now supports exponentially weighted moving averages for numeric lists. Choose decay by smoothing factor, span, center of mass, or half-life, and control adjusted weighting and null handling:

```tql
from {xs: [1, null, 3, 4]}
select result = xs.ewma(span=3, ignore_nulls=true)
```

```tql
{result: [1.0, 1.0, 2.3333333333333335, 3.2857142857142856]}
```

Use a duration half-life with a matching timestamp list to account for irregular sampling intervals:

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

The companion functions `ewm_variance` and `ewm_stddev` compute the exponentially weighted variance and standard deviation with the same options, plus a `bias` option that switches from the debiased to the biased weighted moment. Together with `ewma`, they turn a smoothed series into a streaming baseline with a built-in [z-score](https://en.wikipedia.org/wiki/Standard_score), measuring the deviation from the exponentially weighted mean in units of the exponentially weighted standard deviation:

```tql
from {xs: [10, 12, 11, 13, 12, 11, 12, 40]}
baseline = xs.ewma(span=20).last()
select zscore = (xs.last() - baseline) / xs.ewm_stddev(span=20).last()
```

```tql
{zscore: 2.0333636871546847}
```

*By @mavam, @codex, and @claude.*

### Non-top-level JSON columns and automatic detection for to_clickhouse

The `json` and `low_cardinality` arguments of `to_clickhouse` now accept nested fields, not just top-level ones:

```tql
to_clickhouse table="events", primary=id, json=file.xattributes
```

Previously, only a top-level field like `json=file` could be forced into a ClickHouse `JSON` column. Nested fields could not be targeted directly, which made it impossible to store a deeply nested, dynamically-shaped sub-object as `JSON` without giving up structure for its surrounding fields.

*By @IyeOnline.*

### Periodicity detection with `autocorrelation`, `periodogram`, and `dominant_period`

Three new functions recover periodic structure from event series, closing the gap that made research-grade beacon detection impractical in pure TQL:

- `autocorrelation(xs, max_lag=int)` computes normalized [autocorrelation](https://en.wikipedia.org/wiki/Autocorrelation) coefficients for lags `0` through `max_lag` (default: half the list length).
- `periodogram(xs)` returns spectral power per period via a [fast Fourier transform](https://en.wikipedia.org/wiki/Fast_Fourier_transform), computing the classical [periodogram](https://en.wikipedia.org/wiki/Periodogram).
- `dominant_period(times, resolution=duration)` bins a list of timestamps at the given resolution and returns the strongest period together with a normalized strength between 0 and 1.

Combined with aggregation, this turns beacon detection into a few lines. The first destination below checks in every 30 seconds, the second connects at irregular intervals, and only the beacon survives the strength filter:

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

*By @mavam.*

### to_clickhouse JSON column detection after ocsf::cast

We have improved the interaction between our `ocsf::` operators and `to_clickhouse`.

`ocsf::cast` will now inform `to_clickhouse` about JSON columns, which enables `to_clickhouse` to create more appropriate tables where freeform OCSF fields will be of JSON type.

*By @IyeOnline.*

## 🔧 Changes

### Automatic NetFlow format detection

`read_auto` now recognizes NetFlow v5, NetFlow v9, and IPFIX byte streams and selects `read_netflow` automatically:

```tql
from_file "capture.nfv9" {
  read_auto
}
```

*By @mavam and @codex.*

## 🐞 Bug fixes

### JSON column support for from_clickhouse

`from_clickhouse` can now read columns of ClickHouse's native `JSON` type. Previously, any query selecting a `JSON` column failed with:

```
ClickHouse error: Unsupported JSON serialization version. Make sure output_format_native_write_json_as_string=1 is set.
```

*By @IyeOnline.*
