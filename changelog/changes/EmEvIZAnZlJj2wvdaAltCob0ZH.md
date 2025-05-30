---
title: "Implement `compress` and `decompress` operators"
type: feature
authors: dominiklohmann
pr: 3443
---

The `compress [--level <level>] <codec>` and `decompress <codec>` operators
enable streaming compression and decompression in pipelines for `brotli`, `bz2`,
`gzip`, `lz4`, and `zstd`.
