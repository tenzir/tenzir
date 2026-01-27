---
title: "Trigger new compaction runs immediately on error"
type: bugfix
author: dominiklohmann
created: 2023-03-10T16:09:24Z
pr: 3006
---

Compaction now retries immediately on failure instead of waiting for the
configured scan interval to expire again.
