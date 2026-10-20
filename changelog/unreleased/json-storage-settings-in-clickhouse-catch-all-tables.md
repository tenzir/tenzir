---
title: JSON storage settings in ClickHouse catch-all tables
type: change
authors:
  - zedoraps
created: 2026-10-19T16:32:06.000000Z
---

In tables with a catch-all column, the `to_clickhouse` operator now accepts the `max_dynamic_paths` and `max_dynamic_types` settings on `JSON` columns, including the catch-all column itself. This lets you lower the path limit for a column with many rarely used paths, such as `unmapped`:

```sql
CREATE TABLE security.events (
  time DateTime64(9, 'UTC'),
  unmapped JSON(max_dynamic_paths=64),
  event JSON COMMENT 'tenzir:catch_all'
) ENGINE = MergeTree ORDER BY time;
```

The `from_clickhouse` operator reads catch-all columns with these settings as well. Type hints and `SKIP` clauses are still rejected, because ClickHouse could change or drop values.
