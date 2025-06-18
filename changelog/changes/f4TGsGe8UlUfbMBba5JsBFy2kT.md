---
title: "Don't send null pointers when erasing whole partitions"
type: bugfix
authors: lava
pr: 2227
---

VAST no longer sometimes crashes when aging or compaction erase whole
partitions.
