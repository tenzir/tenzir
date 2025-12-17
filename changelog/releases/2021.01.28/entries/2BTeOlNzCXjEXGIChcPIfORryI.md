---
title: "Check that disk budget was not specified as non-string"
type: bugfix
author: lava
created: 2021-01-13T12:36:49Z
pr: 1278
---

Disk monitor quota settings not ending in a 'B' are no longer silently
discarded.
