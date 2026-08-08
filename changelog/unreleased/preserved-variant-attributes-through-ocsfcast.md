---
title: to_clickhouse JSON column detection after ocsf::cast
type: feature
authors:
  - IyeOnline
created: 2026-07-29T15:25:04.125153Z
---

We have improved the interaction between our `ocsf::` operators and `to_clickhouse`.

`ocsf::cast` will now inform `to_clickhouse` about JSON columns, which enables
`to_clickhouse` to create more appropriate tables where freeform OCSF fields
will be of JSON type.
