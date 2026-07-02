---
title: Hive-partitioned filesystem sinks drop partition columns
type: breaking
authors:
  - raxyte
prs:
  - 6400
created: 2026-07-01T11:39:43.520966Z
---

Filesystem sink operators that use `partition_by` now omit the partition fields
from the written payload. This is a breaking change for pipelines that expected
those fields inside each written file. The values remain encoded in the
hive-style directory components, such as `region=us/`, so readers that recover
partition columns from the path no longer see duplicate fields.
