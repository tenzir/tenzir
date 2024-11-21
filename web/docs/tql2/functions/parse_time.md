# parse_time

Parses a time from a string that follows a specific format.

```tql
parse_time(input: string, format: string) -> time
```

## Description

The `parse_time` function matches the given `input` string against the `format` to construct a timestamp.

### `input: string`

The input string from which the timestamp should be extracted.

### `format: string`

The string that specifies the format of `input`, for example `"%m-%d-%Y"`. The
allowed format specifiers are the same as for `strptime(3)`:
<!-- TODO: Properly describe this or link something. -->

## Examples

### Parse a timestamp

```tql
from {
  x: "2024-12-31+12:59:42",
}
x = x.parse_time("%Y-%m-%d+%H:%M:%S")
```
```tql
{x: 2024-12-31T12:59:42.000000}
```

## See Also

[`format_time`](format_time.md)
