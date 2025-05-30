---
title: "Fix reopening ports while subprocesses are open"
type: bugfix
authors: tobim
pr: 4170
---

We fixed a bug that prevented restarts of pipelines containing a listening
connector under specific circumstances.
