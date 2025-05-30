---
title: chart_bar
---

Plots events on an bar chart.

```tql
chart_bar x|label=field, y|value=any, [x_min=any, x_max=any,
          y_min=any, y_max=any, resolution=duration, fill=any,
          x_log=bool, y_log=bool, group=any, position=string]
```

## Description

Visualizes events with an bar chart on the [Tenzir
Platform](https://app.tenzir.com).

### `x|label = field`

Label for each bar.

### `y|value = any`

Positions on the y-axis for each data point.
Multiple data points for the same group can be be aggregated using an
[aggregation function](/reference/functions#aggregation).

Multiple `y` values and their labels can be specified by using the record
syntax: `{name: value, ...}`.

For example, `y = {"Avg. Load": mean(load)}` calculates the
[mean](/reference/functions/mean) of the `load` field and labels it as `Avg. Load`.

### `x_min = any (optional)`

If specified, only charts events where `x >= x_min`.
If `resolution` is specified, `x_min` is _floored_ to create a full bucket.

### `x_max = any (optional)`

If specified, only charts events where `x <= x_max`.
If `resolution` is specified, `x_max` is _ceiled_ to create a full bucket.

### `y_min = any (optional)`

If specified, any `y` values less than `y_min` will appear clipped out of the
chart.

### `y_max = any (optional)`

If specified, any `y` values greater than `y_max` will appear clipped out of the
chart.

### `resolution = duration (optional)`

This option can be specified to create buckets of the given resolution on the
x-axis. An aggregation function must be specified to combine values in the same
bucket when `resolution` is specified.

For example, if the resolution is set to `15min`, the `x` values are _floored_
to create buckets of 15 minutes. Any aggregations specified act on that bucket.

### `fill = any (optional)`

Optional value to fill gaps and replace `null`s with.

### `x_log = bool (optional)`

If `true`, use a logarithmic scale for the x-axis.

Defaults to `false`.

### `y_log = bool (optional)`

If `true`, use a logarithmic scale for the y-axis.

Defaults to `false`.

### `group = any (optional)`

Optional expression to group the aggregations with.

### `position = string (optional)`

Determines how the `y` values are displayed. Possible values:

- `grouped`
- `stacked`

Defaults to `grouped`.

## Examples

### Chart count of events imported for every unique schema

```tql
metrics "import"
chart_bar x=schema, y=sum(events), x_min=now()-1d
```

## See Also

[`chart_area`](/reference/operators/chart_area),
[`chart_line`](/reference/operators/chart_line),
[`chart_pie`](/reference/operators/chart_pie)
