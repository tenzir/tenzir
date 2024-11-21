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

| Format Specifier   | Description                                                                                                                                                                                                                                                                                      |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `%%`              | The `%` character.                                                                                                                                                                                                                                                                              |
| `%a` or `%A`      | The name of the day of the week according to the current locale, in abbreviated form or the full name.                                                                                                                                                                                           |
| `%b` or `%B` or `%h` | The month name according to the current locale, in abbreviated form or the full name.                                                                                                                                                                                                          |
| `%c`              | The date and time representation for the current locale.                                                                                                                                                                                                                                         |
| `%C`              | The century number (0–99).                                                                                                                                                                                                                                                                       |
| `%d` or `%e`      | The day of the month (1–31).                                                                                                                                                                                                                                                                     |
| `%D`              | Equivalent to `%m/%d/%y`. This is the American-style date, confusing to non-Americans, especially since `%d/%m/%y` is widely used in Europe. The ISO 8601 standard format is `%Y-%m-%d`.                                                                                                         |
| `%H`              | The hour (0–23).                                                                                                                                                                                                                                                                                 |
| `%I`              | The hour on a 12-hour clock (1–12).                                                                                                                                                                                                                                                              |
| `%j`              | The day number in the year (1–366).                                                                                                                                                                                                                                                              |
| `%m`              | The month number (1–12).                                                                                                                                                                                                                                                                         |
| `%M`              | The minute (0–59).                                                                                                                                                                                                                                                                               |
| `%n`              | Arbitrary whitespace.                                                                                                                                                                                                                                                                            |
| `%p`              | The locale's equivalent of AM or PM. (Note: there may be none.)                                                                                                                                                                                                                                  |
| `%r`              | The 12-hour clock time (using the locale's AM or PM). In the POSIX locale, equivalent to `%I:%M:%S %p`. If `t_fmt_ampm` is empty in the `LC_TIME` part of the current locale, the behavior is undefined.                                                                                         |
| `%R`              | Equivalent to `%H:%M`.                                                                                                                                                                                                                                                                           |
| `%S`              | The second (0–60; 60 may occur for leap seconds; earlier also 61 was allowed).                                                                                                                                                                                                                   |
| `%t`              | Arbitrary whitespace.                                                                                                                                                                                                                                                                            |
| `%T`              | Equivalent to `%H:%M:%S`.                                                                                                                                                                                                                                                                        |
| `%U`              | The week number with Sunday as the first day of the week (0–53). The first Sunday of January is the first day of week 1.                                                                                                                                                                         |
| `%w`              | The ordinal number of the day of the week (0–6), with Sunday = 0.                                                                                                                                                                                                                                |
| `%W`              | The week number with Monday as the first day of the week (0–53). The first Monday of January is the first day of week 1.                                                                                                                                                                         |
| `%x`              | The date, using the locale's date format.                                                                                                                                                                                                                                                        |
| `%X`              | The time, using the locale's time format.                                                                                                                                                                                                                                                        |
| `%y`              | The year within the century (0–99). When a century is not otherwise specified, values in the range 69–99 refer to years in the 20th century (1969–1999); values in the range 00–68 refer to years in the 21st century (2000–2068).                                                               |
| `%Y`              | The year, including the century (e.g., 1991).                                                                                                                                                                                                                                                    |
| `%Ec`             | The locale's alternative date and time representation.                                                                                                                                                                                                                                          |
| `%EC`             | The name of the base year (period) in the locale's alternative representation.                                                                                                                                                                                                                   |
| `%Ex`             | The locale's alternative date representation.                                                                                                                                                                                                                                                    |
| `%EX`             | The locale's alternative time representation.                                                                                                                                                                                                                                                    |
| `%Ey`             | The offset from `%EC` (year only) in the locale's alternative representation.                                                                                                                                                                                                                    |
| `%EY`             | The full alternative year representation.                                                                                                                                                                                                                                                        |
| `%Od` or `%Oe`    | The day of the month using the locale's alternative numeric symbols; leading zeros are permitted but not required.                                                                                                                                                                               |
| `%OH`             | The hour (24-hour clock) using the locale's alternative numeric symbols.                                                                                                                                                                                                                         |
| `%OI`             | The hour (12-hour clock) using the locale's alternative numeric symbols.                                                                                                                                                                                                                         |
| `%Om`             | The month using the locale's alternative numeric symbols.                                                                                                                                                                                                                                        |
| `%OM`             | The minutes using the locale's alternative numeric symbols.                                                                                                                                                                                                                                      |
| `%OS`             | The seconds using the locale's alternative numeric symbols.                                                                                                                                                                                                                                      |
| `%OU`             | The week number of the year (Sunday as the first day of the week) using the locale's alternative numeric symbols.                                                                                                                                                                                |
| `%Ow`             | The ordinal number of the day of the week (Sunday=0), using the locale's alternative numeric symbols.                                                                                                                                                                                             |
| `%OW`             | The week number of the year (Monday as the first day of the week) using the locale's alternative numeric symbols.                                                                                                                                                                                |
| `%Oy`             | The year (offset from `%C`) using the locale's alternative numeric symbols.                                                                                                                                                                                                                      |

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
