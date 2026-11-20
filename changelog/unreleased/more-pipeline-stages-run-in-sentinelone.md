---
title: More pipeline stages run in SentinelOne
type: feature
authors:
  - mavam
created: 2026-11-20T11:32:00.998311Z
---

`from_sentinelone_data_lake` now computes more of the pipeline after it in SentinelOne: arithmetic, computed group keys and arguments, the aggregation functions `count_if`, `min`, `max`, `sum`, and `mean`, and the functions `abs`, `ceil`, `floor`, `sqrt`, `exp`, `log`, `to_lower`, `to_upper`, `length_chars`, `trim`, `trim_start`, `trim_end`, `slice`, `is_empty`, `year`, `month`, `day`, `hour`, and `minute`. For example:

```tql
from_sentinelone_data_lake "https://example.sentinelone.net",
  token=secret("sentinelone-token")
where event.type == "File Rename"
hour = timestamp.hour()
summarize hour, large=count_if(tgt.file.size, v => v > 1000),
  avg_kb=mean(tgt.file.size / 1024)
```

This pipeline runs as a single PowerQuery:

```
| filter (event.type == "File Rename")
| let __tenzir_column_0 = number(strftime(timestamp, "%H", "UTC")),
      __tenzir_column_2 = (tgt.file.size / 1024)
| group __tenzir_column_1 = count(((tgt.file.size != null) and (tgt.file.size > 1000))),
        __tenzir_column_3 = avg(__tenzir_column_2)
  by __tenzir_column_0
| limit 1600000
```

These stages run with SentinelOne's semantics. For example, functions and arithmetic treat `null` as `0` or `""`, numbers are doubles, and an aggregation without a group key returns no event for empty input. Use `pushdown=false` to compute them in Tenzir instead. `stddev` and `variance` still run in Tenzir, since PowerQuery provides sample `stddev` and no verified population variance or deviation function.
