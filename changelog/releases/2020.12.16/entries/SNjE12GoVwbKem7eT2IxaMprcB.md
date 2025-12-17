---
title: "Replace PID file if process does not exist"
type: change
author: tobim
created: 2020-10-31T21:11:35Z
pr: 1128
---

VAST no longer requires you to manually remove a stale PID file from a no-longer
running `vast` process. Instead, VAST prints a warning and overwrites the old
PID file.
