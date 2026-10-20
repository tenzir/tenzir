---
title: Catch-all prefilters read only the referenced path
type: change
authors:
  - zedoraps
created: 2026-10-20T12:30:00.000000Z
---

Prefilters that `from_clickhouse` sends for fields in a catch-all table now make ClickHouse read only the referenced path of the `JSON` column, instead of the whole column. Filters on catch-all fields over large tables complete much faster and use far less memory on the ClickHouse server. The set of filters that reach ClickHouse is unchanged.

A column whose name starts with the name of a `JSON` column and a dot, such as `event.category` next to the catch-all column `event`, shadows the fields below that prefix, because ClickHouse resolves `event.category` to the column. Filters on such fields now stay in Tenzir, and `from_clickhouse` emits a warning that names the shadowing column.
