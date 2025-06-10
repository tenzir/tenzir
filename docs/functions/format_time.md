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

| Specifier | Description                            | Example                   |
| :-------: | :------------------------------------- | :------------------------ |
|   `%%`    | A literal `%` character                | `%`                       |
|   `%a`    | Abbreviated or full weekday name       | `Mon`, `Monday`           |
|   `%A`    | Equivalent to `%a`                     | `Mon`, `Monday`           |
|   `%b`    | Abbreviated or full month name         | `Jan`, `January`          |
|   `%B`    | Equivalent to `%b`                     | `Jan`, `January`          |
|   `%c`    | Date and time representation           | `Mon Jan 1 12:00:00 2024` |
|   `%C`    | Century as a decimal number            | `20`                      |
|   `%d`    | Day of the month with zero padding     | `01`, `31`                |
|   `%D`    | Equivalent to `%m/%d/%y`               | `01/31/24`                |
|   `%e`    | Day of the month with space padding    | ` 1`, `31`                |
|   `%F`    | Equivalent to `%Y-%m-%d`               | `2024-01-31`              |
|   `%g`    | Last two digits of ISO week-based year | `24`                      |
|   `%G`    | ISO week-based year                    | `2024`                    |
|   `%h`    | Equivalent to `%b`                     | `Jan`                     |
|   `%H`    | Hour in 24-hour format                 | `00`, `23`                |
|   `%I`    | Hour in 12-hour format                 | `01`, `12`                |
|   `%j`    | Day of year                            | `001`, `365`              |
|   `%m`    | Month number                           | `01`, `12`                |
|   `%M`    | Minutes                                | `00`, `59`                |
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
