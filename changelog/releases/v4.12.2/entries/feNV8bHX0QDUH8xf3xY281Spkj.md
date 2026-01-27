---
title: "Fix reopening ports while subprocesses are open"
type: bugfix
author: tobim
created: 2024-04-30T12:08:39Z
pr: 4170
---

We fixed a bug that prevented restarts of pipelines containing a listening
connector under specific circumstances.
