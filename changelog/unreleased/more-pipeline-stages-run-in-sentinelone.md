---
title: More pipeline stages run in SentinelOne
type: feature
authors:
  - mavam
created: 2026-11-20T11:32:00.998311Z
---

`from_sentinelone_data_lake` now computes more of the pipeline after it in SentinelOne: arithmetic, computed group keys and arguments, the aggregation functions `count_if`, `count_distinct`, `min`, `max`, `sum`, `mean`, `stddev`, `variance`, `all`, and `any`, and the functions `abs`, `ceil`, `floor`, `sqrt`, `exp`, `log`, `to_lower`, `to_upper`, `length_chars`, `trim`, `trim_start`, `trim_end`, `slice`, `is_empty`, `year`, `month`, `day`, `hour`, and `minute`. For example:

```tql
from_sentinelone_data_lake "https://example.sentinelone.net",
  token=secret("sentinelone-token")
where event.type == "File Rename"
hour = timestamp.hour()
summarize hour, files=count_distinct(tgt.file.name),
  large=count_if(tgt.file.size, v => v > 1000),
  avg_kb=mean(tgt.file.size / 1024)
```

This pipeline runs as a single PowerQuery:

```
| filter (event.type == "File Rename")
| let __tenzir_column_0 = number(strftime(timestamp, "%H", "UTC")),
      __tenzir_column_3 = (tgt.file.size / 1024)
| group __tenzir_part_0 = count(((tgt.file.size != null) and (tgt.file.size > 1000))),
        __tenzir_part_1 = sum(__tenzir_column_3),
        __tenzir_part_2 = count(__tenzir_column_3)
  by __tenzir_column_0, __tenzir_distinct_0 = tgt.file.name
| group __tenzir_column_1 = count(__tenzir_distinct_0),
        __tenzir_column_2 = sum(__tenzir_part_0),
        __tenzir_column_4 = (sum(__tenzir_part_1) / sum(__tenzir_part_2))
  by __tenzir_column_0
| limit 1600000
```

Population `stddev` and `variance` rescale PowerQuery's sample `stddev` with `count`, `all` and `any` use conditional counts, and a single `count_distinct` uses two `group` stages excluding `null` (falling back to approximate `estimate_distinct` alongside `stddev`, `variance`, or a second distinct field). These stages run with SentinelOne's semantics: functions and arithmetic treat `null` as `0` or `""`, numbers are doubles, and an aggregation without a group key returns no event for empty input. Use `pushdown=false` to compute them in Tenzir instead.
