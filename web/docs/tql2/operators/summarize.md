# summarize

Groups events and applies aggregate functions on each group.

```tql
summarize (group|aggeration)...
```

## Description

The `summarize` operator groups events according to a grouping expression and
applies the aggregation functions over each group. The operator consumes the
entire input before producing an output.

The order of the output fields follows the sequence of the provided arguments.
Unspecified fields are dropped.

:::note Potentially High Memory Usage
Take care when using this operator with large inputs.
:::

### `group`

Syntactically, `group` has the form `x` or `y=x`, where `x` and `y` are field names.

### `aggregation`

Aggregation functions compute a single value of one or more columns in a given
group. Syntactically, `aggregation` has the form `[y=]f(...)` where `f` is the
aggregation function and `y=` is an optional rename.

By default, the name for the new field `aggregation` is its string
representation, e.g., `min(timestamp)`. You can specify a different name by
prepending a field assignment, e.g., `min_ts=min(timestamp)`.

The following aggregation functions are available:

- `sum`: Computes the sum of all grouped values.
- `min`: Computes the minimum of all grouped values.
- `max`: Computes the maximum of all grouped values.
- `any`: Computes the disjunction (OR) of all grouped values. Requires the
  values to be booleans.
- `all`: Computes the conjunction (AND) of all grouped values. Requires the
  values to be booleans.
- `mean`: Computes the mean of all grouped values.
- `median`: Computes the approximate median of all grouped values with a
  t-digest algorithm.
- `quantile`: Computes the `q` quantile. E.g. `quantile(x, q=0.2)`
- `stddev`: Computes the standard deviation of all grouped values.
- `variance`: Computes the variance of all grouped values.
- `distinct`: Creates a sorted list of all unique grouped values that are not
  null.
- `collect`: Creates a list of all grouped values that are not null.
- `sample`: Takes the first of all grouped values that is not null.
- `count`: Counts all grouped values that are not null. If no argument is
  given, returns the number of events in the group.
- `count_distinct`: Counts all distinct grouped values that are not null.

## Examples

Compute the sum of `x` over all events:

```tql
from [{x: 1}, {x: 2}]
summarize x=sum(x)
// this == {x: 3}
```

Group over `y` and compute the sum of `x` over each group:
```tql
from [
  {x: 0, y: 0, z: 1},
  {x: 1, y: 1, z: 2},
  {x: 1, y: 1, z: 3},
]
summarize y, x=sum(x)
// {y: 0, x: 0}
// {y: 1, x: 2}
```

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

Compute minimum, maximum of the `timestamp` field per `src_ip` group:

```tql
summarize src_ip, min(timestamp), max(timestamp)
```

Compute minimum, maximum of the `timestamp` field over all events:

```tql
summarize min(timestamp), max(timestamp)
```

Create a boolean flag `originator` that is `true` if any value in the group is
`true`:

```tql
summarize src_ip, originator=any(is_orig)
```

Create 1-hour groups and produce a summary of network traffic between host
pairs:

```tql
ts = round(ts, 1h)
summarize ts, src_ip, dest_ip, sum(bytes_in), sum(bytes_out)
```
