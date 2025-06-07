---
title: parse_time
category: Time & Date
example: '"10/11/2012".parse_time("%d/%m/%Y")'
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

| Specifier | Description                            | Example                   |
| :-------: | :------------------------------------- | :------------------------ |
|   `%%`    | A literal `%` character                | `%`                       |
|   `%a`    | Abbreviated weekday name               | `Mon`                     |
|   `%A`    | Full weekday name                      | `Monday`                  |
|   `%b`    | Abbreviated month name                 | `Jan`                     |
|   `%B`    | Full month name                        | `January`                 |
|   `%c`    | Date and time representation           | `Mon Jan 1 12:00:00 2024` |
|   `%C`    | Century (year divided by 100)          | `20`                      |
|   `%d`    | Day of month with zero padding         | `01`, `31`                |
|   `%D`    | Equivalent to `%m/%d/%y`               | `01/31/24`                |
|   `%e`    | Day of month with space padding        | ` 1`, `31`                |
|   `%F`    | Equivalent to `%Y-%m-%d`               | `2024-01-31`              |
|   `%g`    | Last two digits of ISO week-based year | `24`                      |
|   `%G`    | ISO week-based year                    | `2024`                    |
|   `%h`    | Equivalent to `%b`                     | `Jan`                     |
|   `%H`    | Hour in 24-hour format                 | `00`, `23`                |
|   `%I`    | Hour in 12-hour format                 | `01`, `12`                |
|   `%j`    | Day of year                            | `001`, `365`              |
|   `%m`    | Month number                           | `01`, `12`                |
|   `%M`    | Minute                                 | `00`, `59`                |
|   `%n`    | Newline character                      | `\n`                      |
|   `%p`    | AM/PM designation                      | `AM`, `PM`                |
|   `%r`    | 12-hour clock time                     | `12:00:00 PM`             |
|   `%R`    | Equivalent to `%H:%M`                  | `23:59`                   |
|   `%S`    | Seconds                                | `00`, `59`                |
|   `%t`    | Tab character                          | `\t`                      |
|   `%T`    | Equivalent to `%H:%M:%S`               | `23:59:59`                |
|   `%u`    | ISO weekday (Monday=1)                 | `1`, `7`                  |
|   `%U`    | Week number (Sunday as first day)      | `00`, `52`                |
|   `%V`    | ISO week number                        | `01`, `53`                |
|   `%w`    | Weekday (Sunday=0)                     | `0`, `6`                  |
|   `%W`    | Week number (Monday as first day)      | `00`, `52`                |
|   `%x`    | Date representation                    | `01/31/24`                |
|   `%X`    | Time representation                    | `23:59:59`                |
|   `%y`    | Year without century                   | `24`                      |
|   `%Y`    | Year with century                      | `2024`                    |
|   `%z`    | UTC offset                             | `+0000`, `-0430`          |
|   `%Z`    | Time zone abbreviation                 | `UTC`, `EST`              |

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
