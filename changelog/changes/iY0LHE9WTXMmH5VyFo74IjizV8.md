---
title: "Trigger new compaction runs immediately on error"
type: bugfix
authors: dominiklohmann
pr: 3006
---

Compaction now retries immediately on failure instead of waiting for the
configured scan interval to expire again.
