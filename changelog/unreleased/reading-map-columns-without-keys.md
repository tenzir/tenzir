---
title: Reading map columns without keys
type: bugfix
authors:
  - zedoraps
  - claude
created: 2026-08-11T12:32:10.272886Z
---

Reading a map column whose rows are all null no longer fails with an internal
error:

```text
error: unexpected internal error: assertion `r` failed
```

Tenzir represents maps as records, deriving the record's fields from the keys
that the map contains. A column that holds no keys at all has no fields to
derive from, which previously went unhandled. Such columns now read as an empty
record that is null in every row. This most visibly affected `read_parquet`.
