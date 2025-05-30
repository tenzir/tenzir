---
title: "Don't abort startup if individual partitions fail to load"
type: bugfix
authors: tobim
pr: 2515
---

VAST now skips unreadable partitions while starting up, instead of aborting the
initialization routine.
