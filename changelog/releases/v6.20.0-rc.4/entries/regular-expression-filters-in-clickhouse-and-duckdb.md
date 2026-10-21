---
title: Regular expression filters in ClickHouse and DuckDB
type: change
authors:
  - mavam
created: 2026-10-02T19:29:42.248425Z
---

The `from_clickhouse` and `from_duckdb` operators now push `match_regex` filters into the database instead of evaluating them in the pipeline:

```tql
from_clickhouse table="logs.events"
where message.match_regex("^ERROR [0-9]+")
```

TQL and both databases use the RE2 library with the same options, so the pattern matches the same text: `.` does not match a newline, and `^` and `$` match only at the start and end of the string. A database bundles its own release of RE2, which may disagree with TQL on rarely used syntax. For ClickHouse `String` values that are not valid UTF-8, the result is undefined.
