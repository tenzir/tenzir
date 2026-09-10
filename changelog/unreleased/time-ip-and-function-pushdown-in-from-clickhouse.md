---
title: Time, IP, and function pushdown in `from_clickhouse`
type: feature
authors:
  - mavam
created: 2026-09-08T18:04:31.806326Z
---

The `from_clickhouse` operator now translates many more of the predicates that
follow it into the `SELECT` it sends to ClickHouse. Beyond numbers, strings,
and booleans, the pushdown covers:

- `time` values against `Date`, `Date32`, `DateTime`, and `DateTime64`
  columns, including columns in a non-UTC time zone.
- `ip` values against `IPv4` and `IPv6` columns, and `in` with a subnet.
- Equality on enum names, UUIDs, and `FixedString` values, and ordering on
  strings.
- Comparisons between two columns of the same kind.
- The `starts_with`, `ends_with`, and `length_bytes` functions, and substring
  search with `"needle" in haystack`.
- Arithmetic that cannot overflow, such as `port + 1` or `bytes / 1024`, and
  expressions of literals such as `2024-01-01 + 1d`.

A `select` of a nested field such as `meta.level` now transfers only that
element of the tuple instead of the whole column. For example,

```tql
from_clickhouse table="logs.events"
where ts > 2024-01-01 and src_ip in 10.0.0.0/8 and status == "high"
select id, message, meta.level
head 100
```

sends a query equivalent to:

```sql
SELECT id, message, CAST(tuple(meta.level), 'Tuple(level Int64)') AS meta,
       ts, src_ip, status
FROM logs.events
WHERE ts > toDateTime('2024-01-01 00:00:00', 'UTC')
  AND src_ip BETWEEN toIPv4('10.0.0.0') AND toIPv4('10.255.255.255')
  AND status = 'high'
LIMIT 100
```

Tables that store IP addresses as `String` now work with `ip` literals in
`table` mode. Comparing a string with an `ip` is a type mismatch in TQL, so
`where src == 1.1.1.1` used to match no rows against such a column. The
operator now adapts the predicate to the column's type: `src == 1.1.1.1` and
`src in [1.1.1.1, ::1]` compare against the canonical text of the address and
are pushed as plain string comparisons, and `src in 10.0.0.0/8` parses the
column in Tenzir behind a prefilter that lets ClickHouse drop the rows it can
rule out.

Every pushed predicate yields the same rows as its local evaluation, including
for `null` values and for values that ClickHouse can store but Tenzir cannot
represent, such as dates past the year 2262. Anything without an exact
translation keeps running locally with unchanged results.
