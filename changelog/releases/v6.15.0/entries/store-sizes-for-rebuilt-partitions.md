---
title: Store sizes for rebuilt and compacted partitions
type: bugfix
authors:
  - jachris
created: 2026-08-31T00:00:00.000000Z
---

The `partitions` operator now reports the correct `diskusage` and `store` for
every partition that a rebuild or a compaction produced.
