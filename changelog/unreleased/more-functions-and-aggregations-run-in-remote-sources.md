---
title: More functions and aggregations run in remote sources
type: change
authors:
  - mavam
created: 2026-11-18T18:52:41.000000Z
---

`from_duckdb`, `from_mysql`, `from_microsoft_sql`, `from_clickhouse`, `from_hydrolix`, `from_microsoft_defender`, `from_azure_log_analytics`, and `from_splunk` now compute many more functions in the source, in filters and in the groups and arguments of a `summarize`:

- Numbers: `ceil`, `floor`, and `round`, which return an `int`, as well as `sqrt`, `exp`, and `log`, also with a base.
- Times: `year`, `month`, `day`, `hour`, `minute`, and `second`, and `since_epoch` in KQL.
- Strings: `capitalize`, `trim_start`, `trim_end`, `replace`, `is_empty`, `slice` with constant bounds that are not negative, and the hashes `hash_md5`, `hash_sha1`, and `hash_sha256` of strings.
- Aggregations: `stddev` and `variance`, `all` and `any` where the source has booleans, and `mean` in KQL.

```tql
from_clickhouse table="connections", host="localhost"
day = ts.day()
summarize day, deviation=stddev(bytes), dropped=any(blocked)
```

Which functions a source computes depends on what it can spell. The functions run with the source's own semantics, such as how it rounds floating-point numbers or maps case. KQL sums now yield `null` instead of 0 when there are no values, and ClickHouse buckets times before 1970 correctly. Use `pushdown=false` to compute everything in Tenzir.
