---
title: "Replace PID file if process does not exist"
type: change
authors: tobim
pr: 1128
---

VAST no longer requires you to manually remove a stale PID file from a no-longer
running `vast` process. Instead, VAST prints a warning and overwrites the old
PID file.
