---
title: Warn instead of silently dropping events in `to_clickhouse`
type: bugfix
authors:
  - IyeOnline
  - claude
prs:
  - 6396
created: 2026-07-07T17:41:00.397519Z
---

`to_clickhouse` now warns instead of silently dropping events in two cases where
it cannot write a row:

- An event carries a `null` for a ClickHouse column that is not nullable. The
  warning names the offending column.
- An event shares no column with the target table, so there is nothing to
  insert (an all-defaults row cannot be expressed on the native insert path).

Previously both were dropped silently while the operator reported success.
