---
title: Faster JSON reads with projection pushdown
type: change
authors:
  - mavam
  - IyeOnline
created: 2026-11-22T18:17:27.738135Z
---

The `read_json`, `read_ndjson`, `read_gelf`, `read_suricata`, and `read_zeek_json` operators now decode only the fields that the rest of the pipeline uses, such as the fields of a downstream `select` and the fields that a `where` references.

For example:

```tql
from_file "events.ndjson" {
  read_ndjson
}
where status == 500
select id, nested.x
```

Unselected JSON fields are skipped without decoding their values, and pipelines that need no fields at all, such as `summarize count()`, count events without decoding them. Validation warnings and schema-mismatch diagnostics for unselected fields may no longer appear, matching `read_parquet` and `read_feather`.
