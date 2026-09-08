---
title: "`from_clickhouse` pushes filters, limits, and field selection into SQL"
type: feature
authors:
  - mavam
created: 2026-09-07T18:00:00Z
---

The `from_clickhouse` operator now translates the pipeline that follows it into
the `SELECT` it sends to ClickHouse. A `where` becomes a `WHERE` clause, a
`select` narrows the selected columns, and a `head` adds a `LIMIT`, so
ClickHouse reads and transfers only what the pipeline needs. For example,

```tql
from_clickhouse table="logs.events"
where severity > 3 or source == "fw" and code in [401, 403]
select id, message
head 100
```

sends a query equivalent to:

```sql
SELECT id, message, severity, source, code
FROM logs.events
WHERE (severity > 3 OR (source = 'fw' AND code IN (401, 403)))
LIMIT 100
```

The translation only covers predicates whose ClickHouse semantics match TQL
exactly: comparisons of a column with a literal of the same kind (numbers,
strings, booleans), `null` checks, `in` with a list of literals, and `and`,
`or`, and `not`. Nullable columns receive a null guard so that `not (x == 1)`
keeps rows where `x` is `null`, as it does in TQL. Nested fields address tuple
elements. Anything else, such as function calls, comparisons against `time` or
`ip` values, or columns with `Enum` or `Decimal` types, keeps running locally
with unchanged results. When you pass `sql` instead of `table`, the query is
sent as is.
