---
title: ClickHouse catch-all columns
type: feature
authors:
  - zedoraps
created: 2026-09-07T15:48:46.79756Z
---

The `to_clickhouse` operator can map events to an existing table and preserve
unmapped fields in a native JSON column. Opt in with a column comment:

```sql
CREATE TABLE events (
  `src.port` UInt32,
  extra JSON COMMENT 'tenzir:catch_all'
) ENGINE = MergeTree ORDER BY tuple();
```

The operator maps nested `src.port` values to the dotted column name and
packs fields without matching writable columns into `extra`. A value that
fails a mapped column's transformation produces a warning and drops the event;
it does not move into `extra`. Missing fields use ClickHouse defaults.
Tables with the marker refresh their column mappings while the pipeline runs.

For both ordinary JSON columns and the catch-all, the existing JSON printer
omits null object fields and null list elements recursively, retaining empty
records and lists. Native ClickHouse JSON determines their stored representation.
Serialized JSON strings retain the existing writer behavior and malformed
objects can fail insertion at the server. The catch-all does not guarantee
an exact structured round trip for every Tenzir value.

Marked inserts enable `json_type_escape_dots_in_keys=1`, requiring ClickHouse
25.8 or later. Use the same setting in read queries to restore literal dotted
keys. ClickHouse interprets the literal key sequence `%2E` as a dot on read.

Appending to existing tables also supports `UInt16`, `UInt32`, `Int8`, `Int16`,
`Int32`, and `Float32` columns without a catch-all marker. These types check
range and precision; incompatible values produce a warning and drop the
affected event. Unmarked `UInt8` columns retain their existing boolean mapping.
