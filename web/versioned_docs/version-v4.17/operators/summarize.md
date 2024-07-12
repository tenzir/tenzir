---
sidebar_custom_props:
  operator:
    transformation: true
---

# summarize

Groups events and applies aggregate functions on each group.

## Synopsis

```
summarize <[field=]aggregation>... 
          [by <extractor>... [resolution <duration>]]
          [timeout <duration>]
          [update-timeout <duration>]
```

## Description

The `summarize` operator groups events according to a grouping expression and
applies an aggregation function over each group. The operator consumes the
entire input before producing an output.

Fields that neither occur in an aggregation function nor in the `by` list
are dropped from the output.

### `[field=]aggregation`

Aggregation functions compute a single value of one or more columns in a given
group. Syntactically, `aggregation` has the form `f(x)` where `f` is the
aggregation function and `x` is a field.

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
- `p99`, `p95`, `p90`, `p75`, `p50`: Computes the 99th, 95th, 90th, 75th, or
  50th percentile of all grouped values with a t-digest algorithm.
- `stddev`: Computes the standard deviation of all grouped values.
- `variance`: Computes the variance of all grouped values.
- `distinct`: Creates a sorted list of all unique grouped values that are not
  null.
- `collect`: Creates a list of all grouped values that are not null.
- `sample`: Takes the first of all grouped values that is not null.
- `count`: Counts all grouped values that are not null.
- `count_distinct`: Counts all distinct grouped values that are not null.

### `by <extractor>`

The extractors specified after the optional `by` clause partition the input into
groups. If `by` is omitted, all events are assigned to the same group.

### `resolution <duration>`

The `resolution` option specifies an optional duration value that specifies the
tolerance when comparing time values in the `by` section. For example, `01:48`
is rounded down to `01:00` when a 1-hour `resolution` is used.

NB: we introduced the `resolution` option as a stop-gap measure to compensate for
the lack of a rounding function. The ability to apply functions in the grouping
expression will replace this option in the future.

### `timeout <duration>`

The `timeout` option specifies how long an aggregation may take, measured per
group in the `by` section from when the group is created, or if no group exists
from the time when first event arrived at the operator.

If values occur again after the timeout, a new group with an independent
aggregation will be created.

### `update-timeout <duration>`

The `update-timeout` functions just like the `timeout` option, but instead of
measuring from the first event of a group the timeout refreshes whenever an
element is added to a group.

## Examples

Group the input by `src_ip` and aggregate all unique `dest_port` values into a
list:

```
summarize distinct(dest_port) by src_ip
```

Same as above, but produce a count of the unique number of values instead of a
list:

```
summarize count_distinct(dest_port) by src_ip
```

Compute minimum, maximum of the `timestamp` field per `src_ip` group:

```
summarize min(timestamp), max(timestamp) by src_ip
```

Compute minimum, maximum of the `timestamp` field over all events:

```
summarize min(timestamp), max(timestamp)
```

Create a boolean flag `originator` that is `true` if any value in the group is
`true`:

```
summarize originator=any(is_orig) by src_ip
```

Create 1-hour groups and produce a summary of network traffic between host
pairs:

```
summarize sum(bytes_in), sum(bytes_out) by ts, src_ip, dest_ip resolution 1 hour
```
