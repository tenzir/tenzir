---
title: Case-insensitive prefix and suffix filters in ClickHouse and DuckDB
type: change
authors:
  - mavam
created: 2026-10-02T16:34:24.100466Z
---

The `from_clickhouse` and `from_duckdb` operators now push case-insensitive `starts_with` and `ends_with` filters into the database instead of evaluating them in the pipeline:

```tql
from_clickhouse table="logs.events"
where message.starts_with("error", ignore_case=true)
```

The database lowercases text one character at a time, which matches TQL for ASCII and most other text. For a few characters, results can differ from evaluating the filter in the pipeline. TQL applies full Unicode case folding, so `"Straße".starts_with("strass", ignore_case=true)` is `true`, while the database finds no match.
