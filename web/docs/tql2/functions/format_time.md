# format_time

Formats a time into a string that follows a specific format.

```tql
format_time(input: time, format: string) -> string
```

## Description

The `format_time` function formats the given `input` time into a string by using the given `format`.

### `input: time`

The input time for which a string should be constructed.

### `format: string`

The string that specifies the desired output format, for example `"%m-%d-%Y"`. The
allowed placeholders are the same as for `strftime`.
<!-- TODO: Properly describe this or link something. -->

## Examples

### Format a timestamp

```tql
from {
  x: 2024-12-31T12:59:42,
}
x = x.format_time("%d.%m.%Y")
```
```tql
{x: "31.12.2024"}
```

## See Also

[`parse_time`](parse_time.md)
