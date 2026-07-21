---
title: Tolerate ClickHouse columns with a default value in `to_clickhouse`
type: feature
authors:
  - IyeOnline
  - claude
prs:
  - 6396
created: 2026-07-07T13:49:08.516559Z
---

When appending to an existing ClickHouse table, `to_clickhouse` now treats any
column with a default value (`DEFAULT`, `MATERIALIZED`, `ALIAS`, or `EPHEMERAL`)
as optional: if the input omits it entirely, the operator leaves it out of the
insert and lets ClickHouse fill in the default instead of dropping the events.
Previously such a column was treated as required, so a batch that did not carry
it was dropped.

This also applies to columns whose type `to_clickhouse` cannot otherwise
represent, such as `IPv4` or an `Enum`. As long as they have a default, the
table is no longer rejected with an `unsupported ClickHouse type` error. If an
event does provide a value for such an unrepresentable column, the value cannot
be converted and the event is dropped with a warning. A column with an
unsupported type and no default is still rejected as before.

Generated columns (`MATERIALIZED` and `ALIAS`) are handled specially: ClickHouse
computes them and rejects explicit values, so `to_clickhouse` never sends them.
A value provided for such a column is ignored with a warning, and ClickHouse
computes the column as defined. Only overridable `DEFAULT` (and `EPHEMERAL`)
columns accept a value from the input.
