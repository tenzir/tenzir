---
title: VAST v2.3.1
authors: lava
date: 2022-10-17
tags: [release, rebuild, performance]
---

[VAST v2.3.1][github-vast-release] is now available. This small bugfix release
addresses an issue where [compaction][compaction] would hang if encountering
invalid partitions that were produced by older versions of VAST when a large
`max-partition-size` was set in combination with badly compressible input data.

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v2.3.1
[compaction]: https://vast.io/docs/use/transform#transform-old-data-when-reaching-storage-quota
