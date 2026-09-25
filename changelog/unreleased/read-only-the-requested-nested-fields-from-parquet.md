---
title: Read only the requested nested fields from Parquet
type: change
authors:
  - mavam
created: 2026-09-25T09:21:56.07085Z
---

`read_parquet` now decodes only the fields of a record that the pipeline uses. Previously, selecting a nested field decoded the entire top-level record that contains it.

For example, this pipeline reads just two leaf columns instead of every field of `src_endpoint` and `traffic`:

```tql
from_file "/data/flows.parquet" {
  read_parquet
}
select src_endpoint.ip, traffic.bytes
```

Wide records, such as the objects of OCSF events, benefit the most. Fields inside lists and maps, and values such as subnets, are still read as a whole.
