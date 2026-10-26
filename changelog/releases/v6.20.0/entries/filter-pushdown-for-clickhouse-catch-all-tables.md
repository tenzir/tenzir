---
title: Filter pushdown for ClickHouse catch-all tables
type: change
authors:
  - mavam
created: 2026-10-16T17:48:57.782232Z
---

The `from_clickhouse` operator now lets ClickHouse narrow the rows of a catch-all table for more filters, reducing the data sent to Tenzir.

Supported filters on scalar columns now reach ClickHouse even when the column name contains dots. Fields in the catch-all support conservative prefilters, while Tenzir still applies the original filter to the reconstructed events:

```tql
from_clickhouse table="security.events"
where src_endpoint.port == 443 and metadata.product.name == "Demo"
```

Prefilters cover positive string and numeric comparisons, boolean equality, `starts_with`, `ends_with`, safe substring searches, mixed-type `in` lists, and `!= null`. Other JSON columns in the same table qualify if they use plain `JSON` or only `max_dynamic_paths` and `max_dynamic_types` settings. Columns with any typed paths or `SKIP` clauses stay locally filtered. The marked catch-all itself still requires bare `JSON`.

Unsupported predicates on JSON fields, including negation, null equality, arithmetic, regular expressions, and IP or timestamp comparisons, remain in Tenzir. Explicit columns still take precedence over catch-all values. Tables without a catch-all keep their existing behavior.
