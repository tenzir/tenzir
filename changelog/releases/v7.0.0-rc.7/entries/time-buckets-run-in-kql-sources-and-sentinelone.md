---
title: Time buckets run in KQL sources and SentinelOne
type: change
authors:
  - mavam
created: 2026-11-17T12:28:55.000000Z
---

`from_microsoft_defender`, `from_azure_log_analytics`, and `from_sentinelone_data_lake` now compute time buckets with `floor` in the service, such as for the groups of a `summarize`. Only the buckets travel to Tenzir, not the events in them:

```tql
from_sentinelone_data_lake "https://<tenant>.sentinelone.net",
  token=secret("sentinelone-token"),
  start=2026-11-16T00:00:00Z,
  end=2026-11-17T00:00:00Z
bucket = floor(timestamp, 7h)
summarize bucket, events=count()
sort bucket
```

```tql
{bucket: 2026-11-15T19:00:00Z, events: 12744}
{bucket: 2026-11-16T02:00:00Z, events: 45996}
{bucket: 2026-11-16T09:00:00Z, events: 44480}
{bucket: 2026-11-16T16:00:00Z, events: 44221}
{bucket: 2026-11-16T23:00:00Z, events: 6378}
```

On a test tenant, this pipeline summarizes a day of some 150,000 events in about 6 seconds. With `pushdown=false`, reading the same events into Tenzir took longer than the default timeout of 3 minutes. Over shorter windows, both produce the same buckets.

SentinelOne computes `bucket` with `timebucket(timestamp, "25200s")`, and KQL with `bin`, as in `bin(TimeGenerated, timespan(36000000000tick))` for `floor(TimeGenerated, 1h)`. The buckets count from the epoch, as in TQL, also before 1970. KQL's `bin` counts from 0001-01-01, which agrees with the epoch only for some widths, such as those that divide two days, so other widths stay in the pipeline for Defender and Log Analytics. Widths that are multiples of a week or a month stay in the pipeline everywhere, since TQL aligns them to the calendar, and so do widths below a second in SentinelOne.

A `timespan` literal of more than 2^53 ticks, some 28 years, now stays in the pipeline, since KQL reads it inexactly.
