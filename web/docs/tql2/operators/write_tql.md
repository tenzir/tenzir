# write_tql

Transforms the input event stream to a TQL notation byte stream.

```tql
write_json [color=bool, oneline=bool]
```

## Description

Transforms the input event stream to a TQL notation byte stream.

:::tip
`write_tql color=true` is the default sink for terminal output.
:::

### `color = bool (optional)`

Colorize the output.

### `oneline = bool (optional)`

Write one event per line, omitting linebreaks and indentation of records.

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
