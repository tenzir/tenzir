---
title: New `from_clickhouse` source operator
type: feature
authors:
  - IyeOnline
prs:
  - 6048
created: 2026-06-23T10:22:25.712457Z
---

The new `from_clickhouse` operator fetches data from a ClickHouse server.
You can either read an entire table:

```tql
from_clickhouse table="events"
```

Or run a custom SQL query directly:

```tql
from_clickhouse sql="SELECT * FROM events WHERE severity >= 3 ORDER BY time DESC"
```
