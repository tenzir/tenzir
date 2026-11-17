---
title: Time buckets run in SQL sources
type: change
authors:
  - mavam
created: 2026-11-15T19:43:16.024223Z
---

`from_duckdb`, `from_microsoft_sql`, `from_clickhouse`, and `from_hydrolix` now compute time buckets with `floor` in the database, such as for the groups of a `summarize`:

```tql
from_clickhouse table="events", host="localhost"
hour = floor(time, 1h)
summarize hour, events=count()
```

The buckets count from the epoch, as in TQL, also before 1970. ClickHouse computes `hour` with flooring division of `toUnixTimestamp64Nano`, and SQL Server with `DATE_BUCKET(second, 3600, …, CAST('1970-01-01' AS datetime2(7)))`, which needs SQL Server 2022. Widths that are multiples of a week or a month stay in the pipeline, since TQL aligns them to the calendar, and so do widths below a second in SQL Server.
