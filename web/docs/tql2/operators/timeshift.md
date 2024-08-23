# timeshift

Adjusts timestamps relative to a given start time, with an optional speedup.

```tql
timeshift field, [start=time, speed=double]
```

## Description

The `timeshift` operator adjusts a series of time values by anchoring them
around a given `start` time.

With `speed`, you can adjust the relative speed of the time series induced by
`field` with a multiplicative factor. This has the effect of making the time
series "faster" for values great than 1 and "slower" for values less than 1.

![Timeshift](timeshift.excalidraw.svg)

### `<field>`

The field containing the timestamp values.

### `start = time (optional)`

The timestamp to anchor the time values around.

Defaults to the first non-null timestamp in `field`.

### `speed = double (optional)`

A constant factor to be divided by the inter-arrival time. For example, 2.0
decreases the event gaps by a factor of two, resulting a twice as fast dataflow.
A value of 0.1 creates dataflow that spans ten times the original time frame.

Defaults to `1.0`.

## Examples

Set the M57 Zeek logs to begin at Jan 1, 1984:

```tql
load_http "https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst" 
decompress "zstd"
read_zeek_tsv
timeshift ts, start=1984-01-01
```

As above, but also make the time span of the trace 100 times longer:

```tql
load_http "https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst" 
decompress "zstd"
read_zeek_tsv
timeshift ts, start=1984-01-01, speed=0.01
```
