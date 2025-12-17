---
title: "Fixed a shutdown crash in `save_tcp`"
type: bugfix
author: tobim
created: 2025-10-29T10:33:49Z
pr: 5540
---

We fixed an issue that caused the `save_tcp` operator to occasionally crash
while shutting down.
