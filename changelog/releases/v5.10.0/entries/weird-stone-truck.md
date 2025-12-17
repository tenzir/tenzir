---
title: "Compression for `write_bitz`"
type: feature
author: dominiklohmann
created: 2025-07-07T18:37:57Z
pr: 5335
---

Tenzir's internal wire format, which is accessible through the `read_bitz` and
`write_bitz` operators, now uses Zstd compression internally, resulting in a
significantly smaller output size. This change is backwards-compatible.
