---
title: Map columns in from_clickhouse
type: feature
authors:
  - mavam
created: 2026-10-15T07:50:00Z
---

The `from_clickhouse` operator now reads ClickHouse `Map(K, V)` columns as records with one field per key, instead of dropping them with a warning. Keys become field names, using their text for non-string keys, and values keep the usual type mapping, so `Map(String, Nullable(String))` yields string fields that are `null` where the map holds `NULL`. Rows may have different keys:

```tql
from_clickhouse table="events"
where attributes.region? == "eu"
```

Filters on map fields run in Tenzir rather than in ClickHouse.
