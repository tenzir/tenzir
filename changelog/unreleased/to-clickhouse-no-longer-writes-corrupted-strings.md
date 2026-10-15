---
title: "`to_clickhouse` no longer writes corrupted strings"
type: bugfix
authors:
  - mavam
created: 2026-10-14T12:29:35.000000Z
---

The `to_clickhouse` operator could write corrupted string values, such as
strings inside `Tuple` columns, because it sent them from memory that it had
already released. It now copies strings into the insert.
