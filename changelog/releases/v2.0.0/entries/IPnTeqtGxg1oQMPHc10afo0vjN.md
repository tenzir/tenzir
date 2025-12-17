---
title: "Reduce the default log queue size for client commands"
type: bugfix
author: tobim
created: 2022-03-31T12:58:12Z
pr: 2176
---

We optimized the queue size of the logger for commands other than `vast start`.
Client commands now show a significant reduction in memory usage and startup
time.
