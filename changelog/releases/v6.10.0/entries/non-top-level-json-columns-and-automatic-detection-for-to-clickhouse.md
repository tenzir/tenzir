---
title: Non-top-level JSON columns and automatic detection for to_clickhouse
type: feature
authors:
  - IyeOnline
created: 2026-07-29T15:24:57.30563Z
---

The `json` and `low_cardinality` arguments of `to_clickhouse` now accept nested
fields, not just top-level ones:

```tql
to_clickhouse table="events", primary=id, json=file.xattributes
```

Previously, only a top-level field like `json=file` could be forced into a
ClickHouse `JSON` column. Nested fields could not be targeted directly, which
made it impossible to store a deeply nested, dynamically-shaped sub-object as
`JSON` without giving up structure for its surrounding fields.
