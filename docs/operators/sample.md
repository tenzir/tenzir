---
title: sample
category: Filter
example: 'sample 30s, max_samples=2k'
---

Dynamically samples events from a event stream.

```tql
sample [period:duration, mode=string, min_events=int, max_rate=int, max_samples=int]
```

## Description

Dynamically samples input data from a stream based on the frequency of
receiving events for streams with varying load.

The operator counts the number of events received in the `period` and applies
the specified function on the count to calculate the sampling rate for the next
period.

### `period: duration (optional)`

The duration to count events in, i.e., how often the sample rate is computed.

The sampling rate for the first window is `1:1`.

Defaults to `30s`.

### `mode = string (optional)`

The function used to compute the sampling rate:

- `"ln"` (default)
- `"log2"`
- `"log10"`
- `"sqrt"`

### `min_events = int (optional)`

The minimum number of events that must be received during the previous sampling
period for the sampling mode to be applied in the current period. If the number
of events in a sample group falls below this threshold, a `1:1` sample rate is
used instead.

Defaults to `30`.

### `max_rate = int (optional)`

The sampling rate is capped to this value if the computed rate is higher than this.

### `max_samples = int (optional)`

The maximum number of events to emit per `period`.

## Examples

### Sample the input every 30s dynamically

Sample a feed `log-stream` every 30s dynamically, only changing rate when more
than 50 events (`min_events`) are received. Additionally, cap the max sampling
rate to `1:500`, i.e., 1 sample for every 500 events or more (`max_rate`).

```tql
subscribe "log-stream"
sample 30s, min_events=50, max_rate=500
```

### Sample metrics every hour

Sample some `metrics` every hour, limiting the max samples per period to 5,000
samples (`max_samples`) and limiting the overall sample count to 100,000 samples
([`head`](/reference/operators/head)).

```tql
subscribe "metrics"
sample 1h, max_samples=5k
head 100k
```

## See Also

[`deduplicate`](/reference/operators/deduplicate)
