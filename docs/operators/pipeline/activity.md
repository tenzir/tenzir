---
title: activity
category: Pipelines
example: 'pipeline::activity range=1d, interval=1h'
---

Summarizes the activity of pipelines.

```tql
pipeline::activity range=duration, interval=duration
```

## Description

:::caution[Internal Operator]
This operator is only for internal usage by the Tenzir Platform. It can change
without notice.
:::

### `range = duration`

The range for which the activity should be fetched. Note that the individual
rates returned by this operator typically represent a larger range because they
are aligned with the interval.

### `interval = duration`

The interval used to summarize the individual throughout rates. Needs to be a
multiple of the built-in storage interval, which is typically `10min`. Also
needs to cleanly divide `range`.

## Schemas

### `tenzir.activity`

| Field       | Type           | Description                                               |
| :---------- | :------------- | :-------------------------------------------------------- |
| `first`     | `time`         | The time of the first throughput rate in the lists below. |
| `last`      | `time`         | The time of the last throughput rate in the lists below.  |
| `pipelines` | `list<record>` | The activity for individual pipelines.                    |

The records in `pipelines` have the following schema:

| Field     | Type     | Description                                                        |
| :-------- | :------- | :----------------------------------------------------------------- |
| `id`      | `string` | The ID uniquely identifying the pipeline this activity belongs to. |
| `ingress` | `record` | The activity at the source of the pipeline.                        |
| `egress`  | `record` | The activity at the destination of the pipeline.                   |

The records `ingress` and `egress` have the following schema:

| Field      | Type           | Description                                              |
| :--------- | :------------- | :------------------------------------------------------- |
| `internal` | `bool`         | Whether this end of the pipeline is considered internal. |
| `bytes`    | `uint64`       | The total number of bytes over the range.                |
| `rates`    | `list<uint64>` | The throughput in bytes/second over time.                |

You can derive the time associated with a given throughput rate with the formula
`first + index*interval`, except the last value, which is associated with
`last`. The recommended way to chart these values is to show a sliding window
over `[last - range, last]`. The value in `bytes` is an approximation for the
total number of bytes inside that window.

## Examples

### Show the activity over the last 20s

```tql
pipeline::activity range=20s, interval=20s
```

```tql
{
  first: 2025-05-07T08:33:40.000Z,
  last: 2025-05-07T08:34:10.000Z,
  pipelines: [
    {
      id: "3b43d497-5f4d-47f4-b191-5f432644d5ba",
      ingress: {
        internal: true,
        bytes: 289800,
        rates: [
          14490,
          14490,
          14490,
        ],
      },
      egress: {
        internal: true,
        bytes: 292360,
        rates: [
          14721.75,
          14514.25,
          14488.8,
        ],
      },
    },
  ],
}
```
