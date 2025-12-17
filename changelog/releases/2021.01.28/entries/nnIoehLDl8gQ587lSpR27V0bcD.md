---
title: "Don't overwrite line content after a read timeout"
type: bugfix
author: tobim
created: 2021-01-14T16:21:10Z
pr: 1276
---

Line based imports correctly handle read timeouts that occur in the middle of a
line.
