---
sidebar_custom_props:
  operator:
    transformation: true
---

# timeshift

Adjusts timestamps relative to a given start time, with an optional speedup.

## Synopsis

```
timeshift [--start <time>] [--speed <factor>] <field>
```

## Description

The `timeshift` operator adjusts a series of time values by anchoring them
around a given start time.

If you do not provide a start time with `--start`, the operator will anchor the
timestamps in `field` to begin with the current wall clock time, as if you
provided `--start now`.

With `--speed`, you can adjust the relative speed of the time series induced by
`field` with a multiplicative factor.

The options `--start` and `--speed` work independently, i.e., you can use them
separately or both together.

### `--start <time>`

The time value to anchor the values around.

Defaults to `now`.

### `--speed <speed>`

A constant factor to be divided by the inter-arrival time. For example, 2.0
decreases the event gaps by a factor of two, resulting a twice as fast dataflow.
A value of 0.1 creates dataflow that spans ten times the original time frame.

Defaults to 1.0.

### `<field>`

The name of the field containing the timestamp values.

## Examples

Anchor the M57 Zeek data set around Jan 1, 1984:

```
from https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst read zeek-tsv
| timeshift --start 1984-01-01 ts
```

As above, but also make the time span of the trace 100 times longer:

```
from https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst read zeek-tsv
| timeshift --start 1984-01-01 --speed 0.01 ts
```
