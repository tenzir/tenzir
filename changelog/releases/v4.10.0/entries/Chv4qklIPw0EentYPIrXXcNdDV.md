---
title: "Fix a TOCTOU bug that caused the index to fail"
type: bugfix
author: dominiklohmann
created: 2024-03-04T17:59:17Z
pr: 3994
---

Tenzir nodes sometimes failed when trying to canonicalize file system paths
before opening them when the disk-monitor or compaction rotated them out. This
is now handled gracefully.
