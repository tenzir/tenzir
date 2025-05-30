---
title: parse_time
---

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

| Specifier | Description |
|:---------:|:-------------|
| `%%`      | The `%` character.
| `%a/%A`   | Day name in abbreviated or full form.
| `%b/%B/%h`| Month name in abbreviated or full form.
| `%c`      | Date and time representation for the locale.
| `%C`      | Century number (0–99).
| `%d/%e`   | Day of the month (1–31).
| `%D`      | Equivalent to `%m/%d/%y` (American style).
| `%H`      | Hour (0–23).
| `%I`      | Hour on a 12-hour clock (1–12).
| `%j`      | Day number in the year (1–366).
| `%m`      | Month number (1–12).
| `%M`      | Minute (0–59).
| `%n`      | Arbitrary whitespace.
| `%p`      | Locale's equivalent of AM or PM.
| `%r`      | 12-hour clock time, e.g., `%I:%M:%S %p`.
| `%R`      | Equivalent to `%H:%M`.
| `%S`      | Second (0–60, leap seconds included).
| `%t`      | Arbitrary whitespace.
| `%T`      | Equivalent to `%H:%M:%S`.
| `%U`      | Week number (Sunday as the first day, 0–53).
| `%w`      | Ordinal day of the week (0–6, Sunday=0).
| `%W`      | Week number (Monday as the first day, 0–53).
| `%x`      | Date in the locale's format.
| `%X`      | Time in the locale's format.
| `%y`      | Year within the century (0–99).
| `%Y`      | Full year (e.g., 1991).
| `%Ec`     | Locale's alternative date and time.
| `%EC`     | Base year name in alternative representation.
| `%Ex`     | Locale's alternative date.
| `%EX`     | Locale's alternative time.
| `%Ey`     | Year offset from `%EC`.
| `%EY`     | Full alternative year.
| `%Od/%Oe` | Day of month with alternative numeric symbols.
| `%OH`     | Hour (24-hour clock) in alternative numeric symbols.
| `%OI`     | Hour (12-hour clock) in alternative numeric symbols.
| `%Om`     | Month with alternative numeric symbols.
| `%OM`     | Minutes with alternative numeric symbols.
| `%OS`     | Seconds with alternative numeric symbols.
| `%OU`     | Week number (Sunday as first day) in alternative numeric symbols.
| `%Ow`     | Ordinal day of the week in alternative numeric symbols.
| `%OW`     | Week number (Monday as first day) in alternative numeric symbols.
| `%Oy`     | Year offset in alternative numeric symbols.

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

[`format_time`](/reference/functions/format_time)
