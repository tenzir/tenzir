---
title: "Fix syslog parser not yielding on infinite streams"
type: bugfix
authors: IyeOnline
pr: 4777
---

We fixed a bug causing the `syslog` parser to never yield events until the input
stream ended.
