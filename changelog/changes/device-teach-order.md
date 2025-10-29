---
title: "Fixed a shutdown crash in `save_tcp`"
type: bugfix
authors: tobim
pr: 5540
---

We fixed an issue that caused the `save_tcp` operator to occasionally crash
while shutting down.
