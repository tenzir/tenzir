---
title: "Don't abort startup if individual partitions fail to load"
type: bugfix
author: tobim
created: 2022-09-23T12:15:50Z
pr: 2515
---

VAST now skips unreadable partitions while starting up, instead of aborting the
initialization routine.
