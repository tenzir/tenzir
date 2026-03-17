---
title: Add store origin metadata to feather files
type: feature
author: tobim
created: 2026-03-17T00:00:00Z
---

Feather store files now include a `TENZIR:store:origin` key in the Arrow table
schema metadata. The value is `"ingest"` for freshly ingested data, `"rebuild"`
for partitions created by the rebuild command, and `"compaction"` for partitions
created by the compaction plugin. This allows external tooling such as `pyarrow`
to distinguish how a partition was produced.
