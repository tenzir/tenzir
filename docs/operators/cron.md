---
title: cron
category: Flow Control
example: 'cron "* */10 * * * MON-FRI" { from "https://example.org" }'
---

Runs a pipeline periodically according to a cron expression.

```tql
cron schedule:string { … }
```

## Description

The `cron` operator performs scheduled execution of a pipeline indefinitely
according to a [cron expression](https://en.wikipedia.org/wiki/Cron).

The executor spawns a new pipeline according to the cadence given by
`schedule`. If the pipeline runs longer than the interval to the next
scheduled time point, the next run immediately starts.

### `schedule: string`

The cron expression with the following syntax:

```
<seconds> <minutes> <hours> <days of month> <months> <days of week>
```

The 6 fields are separated by a space. Allowed values for each field are:

| Field | Value range* | Special characters | Alternative Literals |
| --- | ---  | --- | --- |
| seconds |  0-59 | `*` `,` `-` | |
| minutes |  0-59 | `*` `,` `-` | |
| hours |  0-23 | `*` `,` `-` | |
| days of | 1-31 | `*` `,` `-` `?` `L` `W` | |
| months | 1-12 | `*` `,` `-` | `JAN` ... `DEC` |
| days of week |  0-6 | `*` `,` `-` `?` `L` `#` | `SUN` ... `SAT` |

The special characters have the following meaning:

| Special character | Meaning | Description |
| --- | --- | --- |
| `*` | all values | selects all values within a field |
| `?` | no specific value | specify one field and leave the other unspecified |
| `-` | range | specify ranges |
| `,` | comma | specify additional values |
| `/` | slash | specify increments |
| `L` | last | last day of the month or last day of the week |
| `W` | weekday | the weekday nearest to the given day |
| `#` | nth |  specify the Nth day of the month |

## Examples

### Fetch the results from an API every 10 minutes

Pull an endpoint on every 10th minute, Monday through Friday:

```tql
cron "* */10 * * * MON-FRI" {
  from "https://example.org/api"
}
publish "api"
```

## See Also

[`every`](/reference/operators/every)
