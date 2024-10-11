---
sidebar_custom_props:
  operator:
    transformation: true
---

# sample

Dynamically samples events from a event stream.

## Synopsis

```
sample [--period <period>] [--mode <mode>] [--min-events <uint>] [--max-rate <uint>] [--max-samples <uint>]
```

## Description

Dynamically samples input data from a stream based on the frequency of
receiving events for streams with varying load.

The operator counts the number of events received in the `period` and applies
the specified function on the count to calculate the sampling rate for the next
period.

### `--period <period>`

The duration to count events in, i.e., how often the sample rate is computed.

The sampling rate for the first window is `1:1`.

Defaults to `30 seconds`.

### `--mode <mode>`

The function used to compute the sampling rate:

- `"ln"` (default)
- `"log2"`
- `"log10"`
- `"sqrt"`

### `--min-events <uint>`

The minimum number of events that must be received during the previous sampling
period for the sampling mode to be applied in the current period. If the number
of events in a sample group falls below this threshold, a `1:1` sample rate is
used instead.

Defaults to `30`.

### `--max-rate <uint>`

The sampling rate is capped to this value if the computed rate is higher than this.

### `--max-samples <uint>`

The maximum number of events to emit per `period`.

## Examples

Sample a feed `log-stream` every 30s dynamically, only changing rate when more
than 50 events (`min-events`) are received. Additionally, cap the max sampling
rate to `1:500`, i.e. 1 sample for every 500 events or more (`max-rate`).

```
subscribe "log-stream" 
| sample --period 30s --min-events=50 --max-rate=500
```
Sample some `metrics` every hour, limiting the max samples per period to 5000
samples (`max-samples`) and limiting the overall sample count to 100k samples
(`head`).

```
subscribe "metrics" 
| sample --period 1h --max-samples 5k | head 100k
```
