# summarize

Groups events and applies aggregate functions to each group.

```tql
summarize (group|aggregation)...
```

## Description

The `summarize` operator groups events according to certain fields and applies
[aggregation functions](../functions.md#aggregation) to each group. The operator
consumes the entire input before producing any output, and may reorder the event
stream.

The order of the output fields follows the sequence of the provided arguments.
Unspecified fields are dropped.

:::note Potentially High Memory Usage
Take care when using this operator with large inputs.
:::

### `group`

To group by a certain field, use the syntax `<field>` or `<field>=<field>`. For
each unique combination of the `group` fields, a single output event will be
returned.

### `aggregation`

The [aggregation functions](../functions.md#aggregation) applied to each group
are specified with `f(…)` or `<field>=f(…)`, where `f` is the name of an
aggregation function (see below) and `<field>` is an optional name for the
result. The aggregation function will produce a single result for each group.

If no name is specified, the aggregation function call will automatically
generate one. If processing continues after `summarize`, we strongly recommend
to specify a custom name.

## Examples

### Compute the sum of a field over all events

```tql
from [{x: 1}, {x: 2}]
summarize x=sum(x)
```

```tql
{x: 3}
```

Group over `y` and compute the sum of `x` for each group:

```tql
from [
  {x: 0, y: 0, z: 1},
  {x: 1, y: 1, z: 2},
  {x: 1, y: 1, z: 3},
]
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
