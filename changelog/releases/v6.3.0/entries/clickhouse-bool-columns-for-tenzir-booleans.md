---
title: ClickHouse Bool columns for Tenzir booleans
type: change
authors:
  - IyeOnline
  - codex
prs:
  - 6048
created: 2026-05-22T12:59:45.594502Z
---

The `to_clickhouse` operator now creates ClickHouse `Bool` columns for Tenzir boolean fields instead of `UInt8` columns.

For example, a Tenzir field such as `active: true` is now stored as `Bool` in ClickHouse when creating a table with `to_clickhouse`.
