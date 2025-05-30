---
title: "Don't overwrite line content after a read timeout"
type: bugfix
authors: tobim
pr: 1276
---

Line based imports correctly handle read timeouts that occur in the middle of a
line.
