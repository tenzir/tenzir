---
title: Uncompressed Feather output
type: feature
author: mavam
pr: 6045
created: 2026-04-30T13:02:52.129138Z
---

The `write_feather` operator now supports `compression_type="uncompressed"`
to disable compression entirely. Previously, only `zstd` and `lz4` were
accepted:

```tql
to_file "events.feather" {
  write_feather compression_type="uncompressed"
}
```
