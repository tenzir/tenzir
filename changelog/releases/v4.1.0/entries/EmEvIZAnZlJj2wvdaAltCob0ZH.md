---
title: "Implement `compress` and `decompress` operators"
type: feature
author: dominiklohmann
created: 2023-08-10T16:25:22Z
pr: 3443
---

The `compress [--level <level>] <codec>` and `decompress <codec>` operators
enable streaming compression and decompression in pipelines for `brotli`, `bz2`,
`gzip`, `lz4`, and `zstd`.
