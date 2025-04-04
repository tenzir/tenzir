# chart_pie

Visualizes events on an pie chart.

```tql
chart_pie label=field, value=any, [group=any]
```

## Description

Visualizes events with an pie chart on the [Tenzir
Platform](https://app.tenzir.com).

### `label = field`

Name of each slice on the chart.

### `value = any`

Value of each slice on the chart.
Multiple data points for the same group can be be aggregated using an
[aggregation function](../functions.md#aggregations).

Multiple `y` values and their labels can be specified by using the record
syntax: `{name: value, ...}`.

For example, `y = {"Avg. Load": mean(load)}` calculates the
[mean](../functions/mean.md) of the `load` field and labels it as `Avg. Load`.

### `group = any (optional)`

Optional expression to group the aggregations with.

## Examples

### Chart count of events imported for every unique schema

```tql
metrics "import"
where timestamp > now() - 1d
chart_pie label=schema, value=sum(events)
```
