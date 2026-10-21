---
title: Append as default mode for to_clickhouse
type: change
authors:
  - IyeOnline
created: 2026-10-13T13:03:25.766600Z
---

The `to_clickhouse` operator now defaults to `mode="append"` and no longer creates missing tables on its own. Create the table in ClickHouse ahead of time, then write to it:

```tql
to_clickhouse table="security.alerts"
```

The `create` and `create_append` modes are deprecated and emit a warning. They continue to work for now, so pipelines that relied on automatic table creation can opt back in with an explicit `mode="create_append"` until they migrate.

Because `primary` and `json` only apply when creating a table, the operator now warns when you set either of them together with `mode="append"`.
