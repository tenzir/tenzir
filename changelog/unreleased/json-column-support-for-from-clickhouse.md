---
title: JSON column support for from_clickhouse
type: bugfix
authors:
  - IyeOnline
created: 2026-07-30T09:18:02.987894Z
---

`from_clickhouse` can now read columns of ClickHouse's native `JSON` type.
Previously, any query selecting a `JSON` column failed with:

```
ClickHouse error: Unsupported JSON serialization version. Make sure output_format_native_write_json_as_string=1 is set.
```
