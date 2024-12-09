---
sidebar_custom_props:
  operator:
    transformation: true
---

# delay

Delays events relative to a given start time, with an optional speedup.

## Synopsis

```
delay [--start <time>] [--speed <factor>] <field>
```

## Description

The `delay` operator replays a dataflow according to a time field by introducing
sleeping periods proportional to the inter-arrival times of the events.

With `--speed`, you can adjust the sleep time of the time series induced by
`field` with a multiplicative factor. This has the effect of making the time
series "faster" for values great than 1 and "slower" for values less than 1.
Unless you provide a start time with `--start`, the operator will anchor the
timestamps in `field` to begin with the current wall clock time, as if you
provided `--start now`.

The diagram below illustrates the effect of applying `delay` to dataflow. If an
event in the stream has a timestamp the precedes the previous event, `delay`
emits it instanstly. Otherwise `delay` sleeps the amount of time to reach the
next timestamp. As shown in the last illustration, the `--speed` factor has a
scaling effect on the inter-arrival times.

![Delay](delay.excalidraw.svg)

The options `--start` and `--speed` work independently, i.e., you can use them
separately or both together.

### `--start <time>`

The timestamp to anchor the time values around.

Defaults to the first non-null timestamp in `field`.

### `--speed <speed>`

A constant factor to be divided by the inter-arrival time. For example, 2.0
decreases the event gaps by a factor of two, resulting a twice as fast dataflow.
A value of 0.1 creates dataflow that spans ten times the original time frame.

Defaults to 1.0.

### `<field>`

The name of the field containing the timestamp values.

## Examples

Replay the M57 Zeek logs with real-world inter-arrival times from the `ts`
column. For example, if event *i* arrives at time *t* and *i + 1* at time *u*,
then the `delay` operator will wait time *u - t* after emitting event *i* before
emitting event *i + 1*. If *t > u* then the operator immediately emits event *i
+ 1*.

```
from https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst read zeek-tsv
| delay ts
```

Replay the M57 Zeek logs at 10 times the original speed. That is, wait *(u - t)
/ 10* between event *i* and *i + 1*, assuming *u > t*.

```
from https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst read zeek-tsv
| delay --speed 10 ts
```

Replay as above, but start delaying only after `ts` exceeds `2021-11-17T16:35`
and emit all events prior to that timestamp immediately.

```
from https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst read zeek-tsv
| delay --start "2021-11-17T16:35" --speed 10 ts
```

Adjust the timestamp to the present, and then start replaying in 2 hours from
now:

```
from https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst read zeek-tsv
| timeshift ts
| delay --start "in 2 hours" ts
```
