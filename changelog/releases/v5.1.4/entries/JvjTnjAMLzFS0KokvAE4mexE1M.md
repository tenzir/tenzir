---
title: "Do not ignore the `tenzir.zstd-compression-level` option"
type: bugfix
author: dominiklohmann
created: 2025-05-07T21:00:03Z
pr: 5183
---

The `tenzir.zstd-compression-level` option now works again as advertised for
setting the Zstd compression level for the partitions written by the `import`
operator. For the past few releases, newly written partitions unconditionally
used the default compression level.
