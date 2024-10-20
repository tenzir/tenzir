# summarize

Groups events and applies aggregate functions to each group.

```tql
summarize (group|aggregation)...
```

## Description

The `summarize` operator groups events according to certain fields and applies
aggregation functions to each group. The operator consumes the entire input
before producing any output.

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

The aggregation functions applied to each group are specified with `f(…)` or
`<field>=f(…)`, where `f` is the name of an aggregation function (see below) and
`<field>` is an optional name for the result. The aggregation function will
produce a single result for each group.

If no name is specified, it will be automatically generated from the aggregation
function call. If processing continues after `summarize`, it is strongly
recommended to specify a custom name.

The following aggregation functions are available and, unless specified
differently, take exactly one argument:

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
- `quantile`: Computes the quantile specified by the named argument `q`, for
  example: `quantile(x, q=0.2)`.
- `stddev`: Computes the standard deviation of all grouped values.
- `variance`: Computes the variance of all grouped values.
- `distinct`: Creates a sorted list without duplicates of all grouped values
  that are not null.
- `collect`: Creates a list of all grouped values that are not null, preserving
  duplicates.
- `first`: Takes the first of all grouped values that is not null.
- `last`: Takes the last of all grouped values that is not null.
- `top`: Takes the most common of all grouped values that is not null.
- `rare`: Takes the least common of all grouped values that is not null.
- `count`: When used as `count()`, simply counts the events in the group. When
  used as `count(x)`, counts all grouped values that are not null.
- `count_distinct`: Counts all distinct grouped values that are not null.

## Examples

Compute the sum of `x` over all events:

```tql
from [{x: 1}, {x: 2}]
summarize x=sum(x)
―――――――――――――――――――――
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
―――――――――――――――――――――
{y: 0, x: 0}
{y: 1, x: 2}
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

Compute minimum and maximum of the `timestamp` field per `src_ip` group:

```tql
summarize min(timestamp), max(timestamp), src_ip
```

Compute minimum and maximum of the `timestamp` field over all events:

```tql
summarize min(timestamp), max(timestamp)
```

Create a boolean flag `originator` that is `true` if any value in the `src_ip`
group is `true`:

```tql
summarize src_ip, originator=any(is_orig)
```

Create 1-hour groups and produce a summary of network traffic between host
pairs:

```tql
ts = round(ts, 1h)
summarize ts, src_ip, dest_ip, sum(bytes_in), sum(bytes_out)
```
