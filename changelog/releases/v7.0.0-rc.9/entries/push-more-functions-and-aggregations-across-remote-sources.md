---
title: Push more functions and aggregations across remote sources
type: change
authors:
  - mavam
created: 2026-11-21T21:25:34.512663Z
---

Federated Search sources now push more TQL functions and aggregations into their target databases and query languages:

- **MySQL and SQL Server** push `all(x)` and `any(x)` (via `MIN`/`MAX` over `0`/`1`) and predicate expressions as group keys or values (via `CASE WHEN ... THEN 1 ELSE 0 END`), decoding results back to booleans. SQL Server also pushes `length_bytes` (`DATALENGTH`) and `hash_md5`/`hash_sha1`/`hash_sha256` (`HASHBYTES`).
- **ClickHouse and DuckDB** push `since_epoch(x)` (`toUnixTimestamp64Nano` in ClickHouse, `to_microseconds(epoch_us(x))` in DuckDB).
- **KQL** pushes `length_bytes(x)` (`string_size(x)`).
- **Splunk (SPL and SPL2)** pushes `all`/`any`, predicates as values (`if(p, "true", "false")`), `capitalize`, `replace` (with a constant literal needle), `slice`, `year`/`month`/`day`/`hour`/`minute`/`second` (`tonumber(strftime(...))`), and whole-second `floor` time buckets.
- **SentinelOne Data Lake** pushes `match_regex` (`matches:matchcase`), `capitalize`, `replace` (case-insensitive regex with a constant literal needle and replacement), millisecond-precision `second` (`number(strftime(x, "%S.%f", "UTC"))`), and predicates as native boolean values in PowerQuery.

For example, `from_splunk` now computes `b = floor(_time, 1h)` and `a = all(ok)` directly in Splunk as `strftime(floor('_time' / 3600) * 3600, "%Y-%m-%dT%H:%M:%S%z")` and `min(eval(case('ok'=="true", 1, 'ok'=="false", 0)))`, and `from_microsoft_sql` computes `h = s.hash_sha256()` as `LOWER(CONVERT(varchar(64), HASHBYTES('SHA2_256', [s]), 2))`.

Pushed expressions run with the target's native semantics; pass `pushdown=false` to evaluate them in Tenzir instead.
