---
title: Column defaults for nulls in ClickHouse
type: bugfix
authors:
  - zedoraps
created: 2026-10-02T18:28:17.516886Z
---

The `to_clickhouse` operator now writes the column default for a `null` in a non-`Nullable` column that has a `DEFAULT`, just like for an event without the field. Previously, such events were dropped with an `incompatible type` warning.

For example, given a table with the column `activity_id Int32 DEFAULT 0`, both of these events now arrive with `activity_id = 0`:

```tql
from {class_uid: 4001, activity_id: null},
     {class_uid: 4001}
to_clickhouse table="ocsf.events", mode="append"
```

ClickHouse evaluates expression defaults as well, such as `DEFAULT class_uid % 1000`. This also applies to `Array`, `Tuple`, and `JSON` columns with a `DEFAULT`, which previously stored an empty value for a `null`. A `null` for a `Nullable` column is still written as `NULL`, and a `null` for a non-`Nullable` column without a `DEFAULT` behaves as before.
