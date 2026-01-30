---
title: Support decompression for Kafka operators
type: bugfix
authors:
  - raxyte
  - claude
pr: 5697
created: 2026-01-30T10:24:46.331421Z
---

Kafka connectors now support decompressing messages with `zstd`, `lz4` and `gzip`.
