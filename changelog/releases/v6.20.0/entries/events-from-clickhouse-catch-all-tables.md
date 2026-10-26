---
title: Events from ClickHouse catch-all tables
type: feature
authors:
  - IyeOnline
  - claude
created: 2026-10-12T14:00:00Z
---

The `from_clickhouse` operator now returns events from a table with a catch-all column the way `to_clickhouse` received them. The fields of the catch-all column move back to the top level of the event, columns with dotted names such as `src_endpoint.ip` become nested fields, and other `JSON` columns become records again:

```tql
from_clickhouse table="security.events"
where metadata.product.name == "Demo"
```

Previously, you had to reassemble every event yourself with `this = {...event.parse_json(), ...this}`, `drop event`, and `unflatten`. Values from `JSON` columns get their types inferred like `parse_json` does, so IP addresses and timestamps come back typed.

Filters on columns without dots in their name still go into the query. All other filters on a catch-all table run in the pipeline, and the query reads every column.
