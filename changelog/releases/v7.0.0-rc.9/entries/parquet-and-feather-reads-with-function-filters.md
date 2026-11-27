---
title: Fewer columns read for filters that call functions
type: change
authors:
  - mavam
created: 2026-11-24T12:00:00.000000Z
---

The `read_parquet` and `read_feather` operators now read only the columns that
a downstream filter needs, even when the filter calls a function, such as
`where string(id) == "1"`. Previously, such a filter made them read every
column.
