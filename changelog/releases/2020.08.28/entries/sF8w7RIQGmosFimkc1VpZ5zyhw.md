---
title: "Protect DB directory with PID lock"
type: feature
author: mavam
created: 2020-08-04T19:09:32Z
pr: 1001
---

VAST now writes a PID lock file on startup to prevent multiple server processes
from accessing the same persistent state. The `pid.lock` file resides in the
`vast.db` directory.
