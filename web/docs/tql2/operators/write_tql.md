# write_tql

Transforms the input event stream to a TQL notation byte stream.

```tql
write_tql [strip=bool, color=bool, oneline=bool,
           strip_null_fields=bool, strip_nulls_in_lists=bool
           strip_empty_records=bool, strip_empty_lists=bool]
```

## Description

Transforms the input event stream to a TQL notation byte stream.

:::tip
`write_tql color=true` is the default sink for terminal output.
:::

### `strip = bool (optional)`

Enables all `strip_*` options.

### `color = bool (optional)`

Colorize the output.

### `oneline = bool (optional)`

Write one event per line, omitting linebreaks and indentation of records.

### `strip_null_fields = bool (optional)`

Strips all fields with a `null` value from records.

### `strip_nulls_in_lists = bool (optional)`

Strips all `null` value to be from lists.

### `strip_empty_records = bool (optional)`

Strips empty records from the output, including those that only became empty
by stripping nulls.

### `strip_empty_lists = bool (optional)`

Strips empty lists from the output, including those that only became empty
by stripping nulls.

## Examples

### Print

```tql
from {activity_id: 16, activity_name: "Query", rdata: 31.3.245.133, dst_endpoint: {ip: 192.168.4.1, port: 53}}
write_tql
```
```tql
{
  activity_id: 16,
  activity_name: "Query",
  rdata: 31.3.245.133,
  dst_endpoint: {
    ip: 192.168.4.1,
    port: 53,
  }
}
```
