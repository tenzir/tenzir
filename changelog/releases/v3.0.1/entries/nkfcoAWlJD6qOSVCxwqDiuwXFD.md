---
title: "Avoid crashing when reading a pre-2.0 partition"
type: bugfix
author: dominiklohmann
created: 2023-03-16T16:38:31Z
pr: 3018
---

VAST no longer crashes when reading an unsupported partition from VAST v1.x.
Instead, the partition is ignored correctly. Since v2.2 VAST automatically
rebuilds partitions in the background to ensure compatibility.
