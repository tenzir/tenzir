---
title: "Don't send null pointers when erasing whole partitions"
type: bugfix
author: lava
created: 2022-04-19T17:06:21Z
pr: 2227
---

VAST no longer sometimes crashes when aging or compaction erase whole
partitions.
