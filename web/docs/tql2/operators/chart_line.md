# chart_line

Chart values on a X-Y line chart.

```tql
chart_line x:any, y:any, [from=any, to=any, resolution=duration, x_log=bool, y_log=bool, stacked=bool]
```

## Description

Chart values on X-Y line chart.

### `x: any`

The values to plot on the X-axis.

### `y: any`

The values to plot on the Y-axes. The values are grouped for each unique `x`
with the given aggregation if specified.

If specified as a record, the field names are used to label the Y-axes.

### `from = any (optional)`

If specified, only charts events where `x > from`.

### `to = any (optional)`

If specified, only charts events where `x < to`.

### `x_log = bool (optional)`

If `true`, use a logarithmic scale for the X-axis.

Defaults to `false`.

### `y_log = bool (optional)`

If `true`, use a logarithmic scale for the Y-axis.

Defaults to `false`.

### `stacked = bool (optional)`

If `true`, stack `y` values on previous ones.

Defaults to `false`.

## Examples

```tql
metrics "operator"
chart_line timestamp, max(running_duration), from=now()-1d, resolution=5m
```

```tql
metrics "api"
chart_line timestamp, {duration: max(running_duration)}, from=now()-1d, resolution=5m
```
