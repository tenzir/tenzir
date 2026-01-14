---
title: "Fix syslog parser not yielding on infinite streams"
type: bugfix
author: IyeOnline
created: 2024-11-18T18:08:40Z
pr: 4777
---

We fixed a bug causing the `syslog` parser to never yield events until the input
stream ended.
