---
title: format_time
category: Time & Date
example: 'ts.format_time("%d/ %m/%Y")'
---

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
allowed format specifiers are the same as for `strftime(3)`:

| Specifier | Description |
|:---------:|-------------|
| `%a`      | Abbreviated name of the day of the week (locale-specific).
| `%A`      | Full name of the day of the week (locale-specific).
| `%b`      | Abbreviated month name (locale-specific).
| `%B`      | Full month name (locale-specific).
| `%c`      | Preferred date and time representation for the current locale.
| `%C`      | Century number (year/100) as a 2-digit integer.
| `%d`      | Day of the month as a decimal number (01–31).
| `%D`      | Equivalent to `%m/%d/%y`.
| `%e`      | Day of the month as a decimal number with a leading space instead of zero.
| `%E`      | Modifier for alternative ("era-based") format.
| `%F`      | ISO 8601 date format (`%Y-%m-%d`).
| `%G`      | ISO week-based year with century.
| `%g`      | ISO week-based year without century (2 digits).
| `%h`      | Equivalent to `%b`.
| `%H`      | Hour (24-hour clock) as a decimal number (00–23).
| `%I`      | Hour (12-hour clock) as a decimal number (01–12).
| `%j`      | Day of the year as a decimal number (001–366).
| `%k`      | Hour (24-hour clock) as a decimal number with leading space (0–23).
| `%l`      | Hour (12-hour clock) as a decimal number with leading space (1–12).
| `%m`      | Month as a decimal number (01–12).
| `%M`      | Minute as a decimal number (00–59).
| `%n`      | Newline character.
| `%O`      | Modifier for alternative numeric symbols.
| `%p`      | AM/PM or corresponding strings for the current locale.
| `%P`      | Like `%p`, but lowercase (e.g., "am", "pm").
| `%r`      | Time in a.m./p.m. notation (locale-specific).
| `%R`      | Time in 24-hour notation (`%H:%M`).
| `%s`      | Seconds since the Unix Epoch (1970-01-01 00:00:00 UTC).
| `%S`      | Second as a decimal number (00–60, allowing leap seconds).
| `%t`      | Tab character.
| `%T`      | Time in 24-hour notation (`%H:%M:%S`).
| `%u`      | Day of the week as a decimal number (1=Monday, 7=Sunday).
| `%U`      | Week number of the year (starting Sunday, range 00–53).
| `%V`      | ISO 8601 week number (range 01–53).
| `%w`      | Day of the week as a decimal number (0=Sunday, 6=Saturday).
| `%W`      | Week number of the year (starting Monday, range 00–53).
| `%x`      | Preferred date representation for the current locale.
| `%X`      | Preferred time representation for the current locale.
| `%y`      | Year without century as a decimal number (00–99).
| `%Y`      | Year with century as a decimal number.
| `%z`      | Numeric timezone offset from UTC (`+hhmm` or `-hhmm`).
| `%Z`      | Timezone name or abbreviation.
| `%+`      | Date and time in `date(1)` format.
| `%%`      | Literal `%` character.

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

[`parse_time`](/reference/functions/parse_time)
