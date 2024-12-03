# chart_line

Chart values on a X-Y line chart.

```tql
chart_line x=any, y=any, [x_min=any, x_max=any, resolution=duration, x_log=bool, y_log=bool, group=any]
```

## Description

Chart values on X-Y line chart.

### `x = any`

The values to plot on the X-axis.

### `y = any`

The values to plot on the Y-axes. The values are grouped for each unique `x`
with the given aggregation if specified.

If specified as a record, the field names are used to label the Y-axes.

### `x_min = any (optional)`

If specified, only charts events where `x > from`.

### `x_max = any (optional)`

If specified, only charts events where `x < to`.

### `x_log = bool (optional)`

If `true`, use a logarithmic scale for the X-axis.

Defaults to `false`.

### `y_log = bool (optional)`

If `true`, use a logarithmic scale for the Y-axis.

Defaults to `false`.

### `group = any (optional)`

Optional field to group the aggregations with.

## Examples
