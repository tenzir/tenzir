---
title: summarize
category: Analyze
example: 'summarize name, sum(amount)'
---

Groups events and applies aggregate functions to each group.

```tql
summarize (group|aggregation)...
```

## Description

The `summarize` operator groups events according to certain fields and applies
[aggregation functions](/reference/functions#aggregation) to each group. By default,
the operator consumes the entire input before producing any output, and may reorder
the event stream.

The order of the output fields follows the sequence of the provided arguments.
Unspecified fields are dropped.

:::note[Potentially High Memory Usage]
Use caution when applying this operator to large inputs. It currently buffers
all data in memory. Out-of-core processing is on our roadmap.
:::

### Options

An optional options record can be passed as the first argument to control
the emission behavior:

- `frequency: duration` - Emit aggregation results at this interval instead
  of only at the end of the input stream.
- `mode: string` - Controls how aggregations are handled between emissions:
  - `"reset"` (default): Reset aggregations after each emission
  - `"cumulative"`: Keep accumulating values across emissions
  - `"update"`: Keep accumulating but only emit when values change

When `frequency` is specified, the operator will emit intermediate results
at the specified interval and always emit final results when the input ends.

### `group`

To group by a certain field, use the syntax `<field>` or `<field>=<field>`. For
each unique combination of the `group` fields, a single output event will be
returned.

### `aggregation`

The [aggregation functions](/reference/functions#aggregation) applied to each group
are specified with `f(…)` or `<field>=f(…)`, where `f` is the name of an
aggregation function (see below) and `<field>` is an optional name for the
result. The aggregation function will produce a single result for each group.

If no name is specified, the aggregation function call will automatically
generate one. If processing continues after `summarize`, we strongly recommend
to specify a custom name.

## Examples

### Compute the sum of a field over all events

```tql
from {x: 1}, {x: 2}
summarize x=sum(x)
```

```tql
{x: 3}
```

Group over `y` and compute the sum of `x` for each group:

```tql
from {x: 0, y: 0, z: 1},
     {x: 1, y: 1, z: 2},
     {x: 1, y: 1, z: 3}
summarize y, x=sum(x)
```

```tql
{y: 0, x: 0}
{y: 1, x: 2}
```

### Gather unique values in a list

Group the input by `src_ip` and aggregate all unique `dest_port` values into a
list:

```tql
summarize src_ip, distinct(dest_port)
```

Same as above, but produce a count of the unique number of values instead of a
list:

```tql
summarize src_ip, count_distinct(dest_port)
```

### Compute min and max of a group

Compute minimum and maximum of the `timestamp` field per `src_ip` group:

```tql
summarize min(timestamp), max(timestamp), src_ip
```

Compute minimum and maximum of the `timestamp` field over all events:

```tql
summarize min(timestamp), max(timestamp)
```

### Check if any value of a group is true

Create a boolean flag `originator` that is `true` if any value in the `src_ip`
group is `true`:

```tql
summarize src_ip, originator=any(is_orig)
```

### Create 1-hour time buckets

Create 1-hour groups and produce a summary of network traffic between host
pairs:

```tql
ts = round(ts, 1h)
summarize ts, src_ip, dest_ip, sum(bytes_in), sum(bytes_out)
```

### Emit aggregations every 5 seconds

Emit the current count and groups every 5 seconds, resetting the
count after each emission:

```tql
summarize {frequency: 5s}, count(), src_ip
```

This is useful for streaming use cases where you want periodic updates
rather than waiting for all input to arrive.

### Cumulative aggregations

Emit running totals every minute, with values accumulating over time:

```tql
summarize {frequency: 1min, mode: "cumulative"}, sum(bytes), dst_ip
```

In cumulative mode, the aggregations continue to grow across emissions:

```tql
{dst_ip: "1.2.3.4", "sum(bytes)": 1000}    // After 1 minute
{dst_ip: "1.2.3.4", "sum(bytes)": 2500}    // After 2 minutes
{dst_ip: "1.2.3.4", "sum(bytes)": 4200}    // After 3 minutes
```

### Update mode for change detection

Emit running totals, but only when values change from the previous emission:

```tql
summarize {frequency: 10s, mode: "update"}, count(), src_ip
```

This mode is useful for monitoring scenarios where you only want to be
notified when metrics actually change, reducing noise from unchanged values.
The first emission for each group is always sent, and subsequent emissions
only occur when aggregation values differ from the previous emission.

For example, if `src_ip` "10.0.0.1" has a count of 5 at t=0s, t=10s, and t=20s,
then increases to 8 at t=30s, only emissions at t=0s (first) and t=30s (changed)
will be produced for that group.

## See Also

[`rare`](/reference/operators/rare),
[`top`](/reference/operators/top)
