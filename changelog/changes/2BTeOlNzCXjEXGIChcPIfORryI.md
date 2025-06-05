---
title: "Check that disk budget was not specified as non-string"
type: bugfix
authors: lava
pr: 1278
---

Disk monitor quota settings not ending in a 'B' are no longer silently
discarded.
