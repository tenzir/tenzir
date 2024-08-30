---
sidebar_custom_props:
  format:
    parser: true
---

# time

Parses a datetime/timestamp using a `strptime`-like format string.

## Synopsis

```
time [--components] [--strict]
     <format>
```

## Description

Returns a timestamp in Unix time.

### `<format>`

`time` is backed by POSIX
[`strptime`](https://man7.org/linux/man-pages/man3/strptime.3.html),
and uses the same format string syntax, with the `"C"` locale.

### `--components`

Instead of a timestamp, returns a record with fields for
`second`, `minute`, `hour`, `day`, `month`, `year`, `utc_offset`,
and `timezone`.

### `--strict`

By default, if some information is missing from the parsed value,
it's defaulted to be today at 00:00 UTC. Additionally, if no year is parsed,
it's set to the previous year if the resulting time had been in the future.

With `--strict`, these defaults aren't used. Not providing values for
years, months, days, hours, and minutes is an error, except when `--components`
is set, where these fields will be `null`.

## Examples

Parse an ISO 8601 timestamp:

```
# Example input:
# 2023-12-18T12:11:05+0100
parse time "%FT%T%z"
# Output:
# 2023-12-18T11:11:05
```

Parse an RFC 3164 syslog message timestamp:

```
# Assuming today is 2023-12-18

# Example input:
# Nov 10 15:10:20
parse time "%b %d %H:%M:%S"
# Output:
# 2023-10-10T15:10:20

# Input in the future:
# Dec 29 15:10:20
parse time "%b %d %H:%M:%S"
# Output (year is 2022):
# 2022-12-29T15:10:20

# With --strict
parse time "%b %d %H:%M:%S"
# Error, missing year
```
