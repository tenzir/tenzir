---
title: "Set minimum timestamp of partitions properly"
type: bugfix
author: dominiklohmann
created: 2023-05-11T13:08:20Z
pr: 3141
---

Some pipelines in compaction caused transformed partitions to be treated as if
they were older than they were supposed to be, causing them to be picked up
again for deletion too early. This bug no longer exists, and compacted
partitions are now considered at most as old as the oldest event before
compaction.
